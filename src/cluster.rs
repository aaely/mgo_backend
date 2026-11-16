//! What two pods have to share to behave as one app.
//!
//! Each pod has its own websocket clients, its own memory and its own copy of the
//! background services. Left alone, a change saved through one pod reaches only
//! that pod's sockets, an editRef issued by one pod is unknown to the other, and
//! every service runs twice. This module is the one place that deals with it:
//!
//! - **Websocket messages** go to this pod's sockets and onto a Redis pub/sub
//!   channel. Every other pod picks them up there and sends them to its own
//!   sockets, applying the same audience rules (topic, dock, role).
//! - **editRefs** and the **part alert list** live in Redis, so whichever pod
//!   takes a request sees the same ones.
//! - **Background services** run on the one pod holding a Redis lock: the active
//!   pod. The other is standby. It serves users like the active one, and takes the
//!   lock over within about 20 seconds if the active pod stops renewing it.
//!
//! Env:
//!   REDIS_SENTINELS    host:port of each sentinel, comma separated
//!   REDIS_MASTER_NAME  the name the sentinels monitor the master under (default "mymaster")
//!   REDIS_PASSWORD     sent to Redis and to the sentinels, when set
//!   REDIS_URL          one Redis with no sentinels, e.g. redis://localhost:6379
//!
//! With neither REDIS_SENTINELS nor REDIS_URL, none of this starts: everything
//! stays in this process's memory and it is always the active pod. That is the
//! mode for a single pod or a local run.
//!
//! When Redis is configured but can't be reached, each pod carries on as a single
//! pod would: messages still reach its own sockets, editRefs fall back to its own
//! memory (so a save works when it lands on the pod that issued the ref), and
//! after ALONE_AFTER it runs the background services itself. Both pods may run
//! them for the length of the outage. That costs some duplicate work, which the
//! services tolerate; the alternative is that nothing flags late trailers or
//! refreshes part alerts until Redis is fixed.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use redis::aio::MultiplexedConnection;
use redis::sentinel::{SentinelClient, SentinelClientBuilder, SentinelServerType};
use redis::{AsyncConnectionConfig, ConnectionAddr, RedisResult, Script};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::sync::{watch, Mutex};
use tokio::time::{timeout, Interval, MissedTickBehavior};
use tokio_tungstenite::tungstenite::Message;

use crate::helpers::{edit_ref_for, MAX_EDIT_REFS_PER_USER};
use crate::structs::{IncomingMessage, MessageData, PartAlert, TrailerRecord, WebSocketList, WsTopics};

/// Pub/sub channel the pods relay websocket messages over.
const CHANNEL: &str = "modms:ws";
/// Holds the name of the active pod.
const LOCK_KEY: &str = "modms:services:active";
/// The part alert list as JSON, written by the active pod's monitoring service.
const ALERTS_KEY: &str = "modms:part_alerts";
/// `modms:pod:<name>`: one per live pod, kept up by its lock attempts.
const POD_KEY_PREFIX: &str = "modms:pod:";

/// How often the active pod renews the lock and the standby tries for it.
const LOCK_TICK: Duration = Duration::from_secs(5);
/// The lock expires this long after its last renewal, which frees it for the standby.
const LOCK_TTL: Duration = Duration::from_secs(15);
/// A pod that has not been able to ask Redis about the lock for this long runs
/// the services itself. Longer than a sentinel failover takes, so a failover
/// doesn't start a second copy on the standby.
const ALONE_AFTER: Duration = Duration::from_secs(30);
/// Every pod publishes a heartbeat each LOCK_TICK. A subscription that hears
/// nothing for this long, not even this pod's own heartbeat, is dead and is reopened.
const SUBSCRIPTION_SILENCE: Duration = Duration::from_secs(20);

/// Per-command and per-connect limit on the command connection.
const REDIS_TIMEOUT: Duration = Duration::from_secs(2);
/// Limit on finding the master through the sentinels and connecting to it.
const CONNECT_DEADLINE: Duration = Duration::from_secs(5);
/// After a failed connect, new attempts are refused for this long, so requests
/// fall back at once instead of each waiting out a timeout.
const RETRY_AFTER: Duration = Duration::from_secs(3);

/// A user's editRefs are dropped this long after the last time any were issued.
/// Every sheet load and shift roll issues them again, so only refs nobody is
/// using any more age out.
const EDIT_REFS_TTL_SECS: u64 = 7 * 24 * 60 * 60;

/// Get-or-create for a batch of editRefs, in one step so two pods can't issue
/// different refs for the same trailer.
/// KEYS: ref->uuid hash, uuid->ref hash. ARGV: max refs, ttl seconds, then
/// (uuid, ref to use if the uuid has none) pairs. Returns each uuid's ref in order.
/// Mirrors helpers::edit_ref_for, including starting over once the user holds
/// the maximum.
const EDIT_REFS_LUA: &str = r"
local out = {}
for i = 3, #ARGV, 2 do
    local ref = redis.call('HGET', KEYS[2], ARGV[i])
    if not ref then
        if redis.call('HLEN', KEYS[1]) >= tonumber(ARGV[1]) then
            redis.call('DEL', KEYS[1], KEYS[2])
        end
        ref = ARGV[i + 1]
        redis.call('HSET', KEYS[1], ref, ARGV[i])
        redis.call('HSET', KEYS[2], ARGV[i], ref)
    end
    out[#out + 1] = ref
end
redis.call('EXPIRE', KEYS[1], ARGV[2])
redis.call('EXPIRE', KEYS[2], ARGV[2])
return out
";

/// Takes the lock if it is free, renews it if this pod holds it. Returns 1 when
/// this pod holds it afterwards.
/// KEYS: lock, this pod's presence key. ARGV: pod name, ttl ms.
/// The presence key is written first on purpose: a write fails on a replica, so
/// a connection left on a demoted master errors here and gets reopened.
const LOCK_LUA: &str = r"
redis.call('SET', KEYS[2], '1', 'PX', ARGV[2])
local holder = redis.call('GET', KEYS[1])
if holder == ARGV[1] then
    redis.call('PEXPIRE', KEYS[1], ARGV[2])
    return 1
end
if not holder then
    redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
    return 1
end
return 0
";

// ── Who a websocket message is for ──

/// Which sockets get a message. Every rule set has to pass; the default is every
/// open socket. Each pod applies it to its own sockets.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Audience {
    topic:       Option<String>,
    dock:        Option<String>,
    onsite_only: bool,
}

impl Audience {
    /// Every open socket, whatever page or topic it is on.
    pub fn everyone() -> Self {
        Self::default()
    }

    /// Only sockets subscribed to `topic`.
    pub fn topic(topic: &str) -> Self {
        Self { topic: Some(topic.to_string()), ..Self::default() }
    }

    /// Only roles that may see `dock_code`: VAA and Universal hear about their
    /// own dock and nothing else (permissions::dock_allowed).
    pub fn dock(dock_code: &str) -> Self {
        Self::default().and_dock(dock_code)
    }

    pub fn and_dock(mut self, dock_code: &str) -> Self {
        self.dock = Some(dock_code.to_string());
        self
    }

    /// Never the vaa/univ roles, whatever the dock.
    pub fn onsite_only(mut self) -> Self {
        self.onsite_only = true;
        self
    }

    fn admits(&self, role: &str, subscribed: Option<&HashSet<String>>) -> bool {
        if let Some(topic) = &self.topic {
            if !subscribed.is_some_and(|s| s.contains(topic)) { return false; }
        }
        if let Some(dock) = &self.dock {
            if !crate::permissions::dock_allowed(role, dock) { return false; }
        }
        !(self.onsite_only && (role.contains("vaa") || role.contains("univ")))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum Body {
    /// One websocket text frame, the same for every recipient.
    Text(String),
    /// A new trailer row. editRefs are per user, so each pod stamps the row with
    /// each recipient's own before sending it.
    AddOn { ws_type: String, trailer: TrailerRecord },
    /// Proves the channel still carries messages. Never delivered.
    Heartbeat,
}

/// What travels over the Redis channel.
#[derive(Debug, Serialize, Deserialize)]
struct Envelope {
    /// The pod that published it, which skips it: its sockets already have it.
    origin:   String,
    /// Counts up per origin. A publish that is retried goes out twice with the
    /// same number, and receivers drop the repeat.
    seq:      u64,
    audience: Audience,
    body:     Body,
}

// ── The Redis connection ──

enum Source {
    Single(redis::Client),
    Sentinel(Mutex<SentinelClient>),
}

impl Source {
    /// None when Redis isn't configured. Panics on a setting that can't be
    /// parsed: better to fail the start than to run two pods that silently
    /// don't share anything.
    fn from_env() -> Option<Source> {
        let var = |name: &str| std::env::var(name).ok().map(|v| v.trim().to_string()).filter(|v| !v.is_empty());

        if let Some(list) = var("REDIS_SENTINELS") {
            let nodes: Vec<ConnectionAddr> = list
                .split(',')
                .map(str::trim)
                .filter(|node| !node.is_empty())
                .map(|node| {
                    let (host, port) = node.rsplit_once(':')
                        .unwrap_or_else(|| panic!("REDIS_SENTINELS entry '{node}' must be host:port"));
                    let port = port.parse::<u16>()
                        .unwrap_or_else(|_| panic!("REDIS_SENTINELS entry '{node}' has an invalid port"));
                    ConnectionAddr::Tcp(host.to_string(), port)
                })
                .collect();
            let master = var("REDIS_MASTER_NAME").unwrap_or_else(|| "mymaster".to_string());
            let mut builder = SentinelClientBuilder::new(nodes, master.as_str(), SentinelServerType::Master)
                .expect("REDIS_SENTINELS is not valid");
            if let Some(password) = var("REDIS_PASSWORD") {
                builder = builder
                    .set_client_to_redis_password(password.as_str())
                    .set_client_to_sentinel_password(password.as_str());
            }
            println!("Cluster: Redis through the sentinels at {list} (master name {master})");
            return Some(Source::Sentinel(Mutex::new(builder.build().expect("REDIS_SENTINELS is not valid"))));
        }

        let url = var("REDIS_URL")?;
        println!("Cluster: one Redis with no sentinels (REDIS_URL)");
        Some(Source::Single(redis::Client::open(url).expect("REDIS_URL is not valid")))
    }
}

struct LinkState {
    conn:     Option<MultiplexedConnection>,
    retry_at: Option<Instant>,
}

struct RedisLink {
    source: Source,
    state:  Mutex<LinkState>,
    /// Whether the last command got through. Only there so an outage is logged
    /// once when it starts and once when it ends, not on every request.
    up:     AtomicBool,
}

impl RedisLink {
    fn new(source: Source) -> Self {
        RedisLink {
            source,
            state: Mutex::new(LinkState { conn: None, retry_at: None }),
            up:    AtomicBool::new(true),
        }
    }

    /// A client for the current master. The sentinels are asked every time, so
    /// after a failover this names the new master.
    async fn master(&self) -> Result<redis::Client, String> {
        match &self.source {
            Source::Single(client) => Ok(client.clone()),
            Source::Sentinel(sentinel) => {
                let mut sentinel = sentinel.lock().await;
                match timeout(CONNECT_DEADLINE, sentinel.async_get_client()).await {
                    Ok(found) => found.map_err(|e| format!("no master from the sentinels: {e}")),
                    Err(_)    => Err("the sentinels did not answer in time".to_string()),
                }
            }
        }
    }

    /// The shared command connection, opened on first use and after a failure.
    async fn connection(&self) -> Result<MultiplexedConnection, String> {
        let mut state = self.state.lock().await;
        if let Some(conn) = &state.conn {
            return Ok(conn.clone());
        }
        if state.retry_at.is_some_and(|at| Instant::now() < at) {
            return Err("not retrying yet".to_string());
        }
        let config = AsyncConnectionConfig::new()
            .set_connection_timeout(Some(REDIS_TIMEOUT))
            .set_response_timeout(Some(REDIS_TIMEOUT));
        let opened = async {
            let client = self.master().await?;
            client.get_multiplexed_async_connection_with_config(&config).await
                .map_err(|e| format!("can't connect to the master: {e}"))
        };
        let opened = timeout(CONNECT_DEADLINE, opened).await
            .unwrap_or_else(|_| Err("connecting took too long".to_string()));
        match opened {
            Ok(conn) => {
                state.conn = Some(conn.clone());
                state.retry_at = None;
                Ok(conn)
            }
            Err(e) => {
                state.retry_at = Some(Instant::now() + RETRY_AFTER);
                Err(e)
            }
        }
    }

    async fn attempt<T, F, Fut>(&self, op: &F) -> Result<T, String>
    where
        F:   Fn(MultiplexedConnection) -> Fut,
        Fut: Future<Output = RedisResult<T>>,
    {
        let conn = self.connection().await?;
        match op(conn).await {
            Ok(value) => Ok(value),
            Err(e) => {
                // Whatever went wrong, the next command starts from a fresh
                // connection to whichever node is master by then
                self.state.lock().await.conn = None;
                Err(e.to_string())
            }
        }
    }

    /// Runs `op`, once more on a fresh connection if it fails: the connection
    /// may have died since its last use (a failover, a restarted Redis), and the
    /// first command after that is the one that finds out. `op` therefore has to
    /// be safe to run twice.
    async fn run<T, F, Fut>(&self, op: F) -> Result<T, String>
    where
        F:   Fn(MultiplexedConnection) -> Fut,
        Fut: Future<Output = RedisResult<T>>,
    {
        let result = match self.attempt(&op).await {
            Ok(value) => Ok(value),
            // The first error is the one worth reporting
            Err(first) => self.attempt(&op).await.map_err(|_| first),
        };
        match &result {
            Ok(_) => {
                if !self.up.swap(true, Ordering::Relaxed) {
                    println!("Cluster: Redis is reachable again");
                }
            }
            Err(e) => {
                if self.up.swap(false, Ordering::Relaxed) {
                    eprintln!("Cluster: Redis is not reachable, carrying on with this pod's own memory: {e}");
                }
            }
        }
        result
    }
}

// ── The cluster ──

pub struct Cluster {
    /// This pod's name: the lock's value and the origin on relayed messages.
    pod:       String,
    ws_list:   WebSocketList,
    ws_topics: WsTopics,
    /// editRef -> trailer uuid, per user. The only store when Redis isn't
    /// configured; otherwise what is used while Redis can't be reached.
    local_edit_refs: Mutex<HashMap<String, HashMap<String, String>>>,
    /// The list the monitoring service last produced on this pod.
    local_alerts: Mutex<Vec<PartAlert>>,
    /// True while this pod is the one that runs the background services.
    active:    watch::Sender<bool>,
    redis:     Option<RedisLink>,
    /// Messages waiting to go out over Redis. None when Redis isn't configured,
    /// as nothing would ever drain it.
    outbox:    Option<UnboundedSender<(Audience, Body)>>,
}

impl Cluster {
    /// Reads the Redis settings and, if there are any, starts the relay and the
    /// lock tasks. Has to be called inside the tokio runtime.
    pub fn start(ws_list: WebSocketList, ws_topics: WsTopics) -> Arc<Cluster> {
        let host = std::env::var("HOSTNAME").ok().filter(|h| !h.trim().is_empty()).unwrap_or_else(|| "pod".to_string());
        // The suffix keeps two runs of one pod name apart (a restarted container)
        let pod = format!("{}-{}", host.trim(), &uuid::Uuid::new_v4().simple().to_string()[..8]);

        let source = Source::from_env();
        let (outbox, queue) = mpsc::unbounded_channel();
        let cluster = Arc::new(Cluster {
            pod,
            ws_list,
            ws_topics,
            local_edit_refs: Mutex::new(HashMap::new()),
            local_alerts:    Mutex::new(Vec::new()),
            // With no Redis there is nobody to share with: always the active pod
            active: watch::channel(source.is_none()).0,
            outbox: source.is_some().then_some(outbox),
            redis:  source.map(RedisLink::new),
        });

        if cluster.redis.is_some() {
            println!("Cluster: this pod is {}", cluster.pod);
            tokio::spawn(cluster.clone().relay_out(queue));
            tokio::spawn(cluster.clone().relay_in());
            tokio::spawn(cluster.clone().hold_lock());
        } else {
            println!("Cluster: no REDIS_SENTINELS or REDIS_URL, so running as a single pod (state in memory, services always on)");
        }
        cluster
    }

    // ── Websocket messages ──

    /// Sends one text frame to `audience` on every pod.
    pub async fn broadcast(&self, audience: Audience, frame: String) {
        self.send(audience, Body::Text(frame)).await;
    }

    /// Sends `{type, data: {message}}`, the shape every server-sent message has,
    /// to `audience` on every pod. `payload` is the message text, usually JSON.
    pub async fn broadcast_message(&self, audience: Audience, message_type: &str, payload: Option<String>) {
        let message = IncomingMessage {
            r#type: message_type.to_string(),
            data:   payload.map(|message| MessageData { message }),
        };
        if let Ok(frame) = serde_json::to_string(&message) {
            self.broadcast(audience, frame).await;
        }
    }

    /// Sends a newly added trailer to `audience` on every pod, each recipient's
    /// copy stamped with their own editRef. A shared payload could only go out
    /// without one, which leaves everyone but the creator with a row they can't
    /// save until a reload fetches its ref.
    pub async fn broadcast_add_on(&self, audience: Audience, ws_type: &str, trailer: &TrailerRecord) {
        self.send(audience, Body::AddOn { ws_type: ws_type.to_string(), trailer: trailer.clone() }).await;
    }

    async fn send(&self, audience: Audience, body: Body) {
        // This pod's sockets first and directly, so they get it at once and
        // whether or not Redis is up
        self.deliver(&audience, &body).await;
        if let Some(outbox) = &self.outbox {
            let _ = outbox.send((audience, body));
        }
    }

    /// Sends `body` to this pod's sockets that `audience` admits.
    async fn deliver(&self, audience: &Audience, body: &Body) {
        // Recipients are collected under the socket locks and sent to after they
        // are released, so no socket lock is held across the editRef lookup below.
        // ws_list is taken before ws_topics everywhere.
        let recipients: Vec<(UnboundedSender<Message>, String)> = {
            let list   = self.ws_list.lock().await;
            let topics = self.ws_topics.lock().await;
            list.iter()
                .filter(|(addr, client)| audience.admits(&client.role, topics.get(*addr)))
                .map(|(_, client)| (client.tx.clone(), client.username.clone()))
                .collect()
        };

        match body {
            Body::Text(frame) => {
                for (tx, _) in &recipients {
                    let _ = tx.send(Message::Text(frame.clone()));
                }
            }
            Body::AddOn { ws_type, trailer } => {
                // One frame per user, shared by every socket that user has open
                let mut frames: HashMap<&str, Option<String>> = HashMap::new();
                for (tx, username) in &recipients {
                    if !frames.contains_key(username.as_str()) {
                        let edit_ref = self.edit_refs_for(username, std::slice::from_ref(&trailer.uuid)).await
                            .pop()
                            .unwrap_or_default();
                        let stamped = TrailerRecord { editRef: edit_ref, ..trailer.clone() };
                        let frame = serde_json::to_string(&stamped).ok().and_then(|data| {
                            serde_json::to_string(&IncomingMessage {
                                r#type: ws_type.clone(),
                                data:   Some(MessageData { message: data }),
                            }).ok()
                        });
                        frames.insert(username.as_str(), frame);
                    }
                    if let Some(Some(frame)) = frames.get(username.as_str()) {
                        let _ = tx.send(Message::Text(frame.clone()));
                    }
                }
            }
            Body::Heartbeat => {}
        }
    }

    /// Publishes queued messages in the order they were sent. One task does all
    /// of it, so a slow or absent Redis never holds up the request that sent the
    /// message, and two updates to one trailer can't overtake each other.
    async fn relay_out(self: Arc<Self>, mut queue: UnboundedReceiver<(Audience, Body)>) {
        let Some(link) = &self.redis else { return };
        let mut seq: u64 = 0;
        // Messages given up on since the last one that got through
        let mut lost: u64 = 0;
        while let Some((audience, body)) = queue.recv().await {
            seq += 1;
            let envelope = Envelope { origin: self.pod.clone(), seq, audience, body };
            let Ok(payload) = serde_json::to_string(&envelope) else { continue };
            let payload = &payload;
            let published = link.run(|mut conn| async move {
                redis::cmd("PUBLISH").arg(CHANNEL).arg(payload).query_async::<()>(&mut conn).await
            }).await;
            match published {
                Ok(()) => {
                    if lost > 0 {
                        eprintln!("Cluster: {lost} websocket message(s) were not relayed to the other pods while Redis was unreachable");
                        lost = 0;
                    }
                }
                Err(_) => {
                    // What queued up while that attempt timed out is stale by the
                    // time Redis is back, and a burst of old updates would repaint
                    // newer rows. Drop it; those screens catch up on their next
                    // load. The outage itself is logged by the link.
                    lost += 1;
                    while queue.try_recv().is_ok() { lost += 1; }
                }
            }
        }
    }

    /// Receives what the other pods publish and sends it to this pod's sockets.
    /// Reconnects for as long as the process lives.
    async fn relay_in(self: Arc<Self>) {
        let mut last_seq: HashMap<String, u64> = HashMap::new();
        let mut was_listening = false;
        loop {
            let ended = self.listen(&mut last_seq, &mut was_listening).await;
            if was_listening {
                eprintln!("Cluster: lost the websocket relay subscription, reconnecting: {ended}");
                was_listening = false;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    /// One subscription, from connect until it fails. Returns why it ended.
    async fn listen(&self, last_seq: &mut HashMap<String, u64>, listening: &mut bool) -> String {
        let Some(link) = &self.redis else { return "Redis is not configured".to_string() };
        let client = match link.master().await {
            Ok(client) => client,
            Err(e) => return e,
        };
        let mut pubsub = match timeout(CONNECT_DEADLINE, client.get_async_pubsub()).await {
            Ok(Ok(pubsub)) => pubsub,
            Ok(Err(e))     => return e.to_string(),
            Err(_)         => return "connecting took too long".to_string(),
        };
        match timeout(CONNECT_DEADLINE, pubsub.subscribe(CHANNEL)).await {
            Ok(Ok(()))  => {}
            Ok(Err(e))  => return e.to_string(),
            Err(_)      => return "subscribing took too long".to_string(),
        }
        *listening = true;
        println!("Cluster: receiving websocket messages from the other pods");

        let mut messages = pubsub.on_message();
        loop {
            let message = match timeout(SUBSCRIPTION_SILENCE, messages.next()).await {
                Ok(Some(message)) => message,
                Ok(None)          => return "the connection closed".to_string(),
                Err(_)            => return format!("nothing heard for {}s", SUBSCRIPTION_SILENCE.as_secs()),
            };
            let Ok(envelope) = serde_json::from_slice::<Envelope>(message.get_payload_bytes()) else {
                eprintln!("Cluster: ignoring a relayed message this version can't read");
                continue;
            };
            if envelope.origin == self.pod || matches!(envelope.body, Body::Heartbeat) {
                continue;
            }
            // A retried publish arrives twice with the same number
            if last_seq.get(&envelope.origin).is_some_and(|&seen| envelope.seq <= seen) {
                continue;
            }
            // One entry per pod start; cleared long before it could matter
            if last_seq.len() >= 64 { last_seq.clear(); }
            last_seq.insert(envelope.origin, envelope.seq);

            self.deliver(&envelope.audience, &envelope.body).await;
        }
    }

    // ── editRefs ──

    fn edit_ref_keys(username: &str) -> (String, String) {
        (format!("modms:editrefs:{username}"), format!("modms:editrefs:uuids:{username}"))
    }

    /// The user's editRef for each uuid, in order, reusing the ones already
    /// issued. A ref is a per-user handle for a trailer the user was shown;
    /// update_live_trailer accepts nothing else.
    pub async fn edit_refs_for(&self, username: &str, uuids: &[String]) -> Vec<String> {
        if uuids.is_empty() {
            return Vec::new();
        }
        if let Some(link) = &self.redis {
            let (refs_key, uuids_key) = Self::edit_ref_keys(username);
            let script = Script::new(EDIT_REFS_LUA);
            // Generated once, so a retry asks for the same refs
            let fresh: Vec<String> = uuids.iter().map(|_| uuid::Uuid::new_v4().to_string()).collect();
            let (script, refs_key, uuids_key, fresh) = (&script, &refs_key, &uuids_key, &fresh);
            let issued = link.run(|mut conn| async move {
                let mut call = script.prepare_invoke();
                call.key(refs_key).key(uuids_key).arg(MAX_EDIT_REFS_PER_USER).arg(EDIT_REFS_TTL_SECS);
                for (uuid, candidate) in uuids.iter().zip(fresh) {
                    call.arg(uuid).arg(candidate);
                }
                call.invoke_async::<Vec<String>>(&mut conn).await
            }).await;
            match issued {
                Ok(refs) if refs.len() == uuids.len() => return refs,
                Ok(refs) => eprintln!("Cluster: Redis returned {} editRefs for {} trailers; using this pod's memory", refs.len(), uuids.len()),
                // Already logged by the link. Refs from this pod's memory still
                // save when the save lands on this pod.
                Err(_) => {}
            }
        }
        let mut all = self.local_edit_refs.lock().await;
        let user_map = all.entry(username.to_string()).or_default();
        uuids.iter().map(|uuid| edit_ref_for(user_map, uuid)).collect()
    }

    /// The trailer uuid behind one of the user's editRefs, if it is one.
    pub async fn resolve_edit_ref(&self, username: &str, edit_ref: &str) -> Option<String> {
        if let Some(link) = &self.redis {
            let (refs_key, _) = Self::edit_ref_keys(username);
            let refs_key = &refs_key;
            let found = link.run(|mut conn| async move {
                redis::cmd("HGET").arg(refs_key).arg(edit_ref).query_async::<Option<String>>(&mut conn).await
            }).await;
            if let Ok(Some(uuid)) = found {
                return Some(uuid);
            }
        }
        // Not in Redis: it may have been issued from this pod's memory while
        // Redis was down
        self.local_edit_refs.lock().await
            .get(username)
            .and_then(|user_map| user_map.get(edit_ref))
            .cloned()
    }

    /// Forgets every editRef the user holds (logout).
    pub async fn clear_edit_refs(&self, username: &str) {
        self.local_edit_refs.lock().await.remove(username);
        if let Some(link) = &self.redis {
            let (refs_key, uuids_key) = Self::edit_ref_keys(username);
            let (refs_key, uuids_key) = (&refs_key, &uuids_key);
            let _ = link.run(|mut conn| async move {
                redis::cmd("DEL").arg(refs_key).arg(uuids_key).query_async::<()>(&mut conn).await
            }).await;
        }
    }

    // ── Part alerts ──

    /// Replaces the alert list. Called by the monitoring service, which runs on
    /// the active pod only, so the list has to be where the standby can read it.
    pub async fn set_part_alerts(&self, alerts: &[PartAlert]) {
        *self.local_alerts.lock().await = alerts.to_vec();
        if let Some(link) = &self.redis {
            let Ok(json) = serde_json::to_string(alerts) else { return };
            let json = &json;
            let _ = link.run(|mut conn| async move {
                redis::cmd("SET").arg(ALERTS_KEY).arg(json).query_async::<()>(&mut conn).await
            }).await;
        }
    }

    /// The current alert list, the same on every pod.
    pub async fn part_alerts(&self) -> Vec<PartAlert> {
        if let Some(link) = &self.redis {
            let stored = link.run(|mut conn| async move {
                redis::cmd("GET").arg(ALERTS_KEY).query_async::<Option<String>>(&mut conn).await
            }).await;
            if let Ok(Some(json)) = stored {
                if let Ok(alerts) = serde_json::from_str::<Vec<PartAlert>>(&json) {
                    // Kept so the standby still has the last list it saw to
                    // serve if Redis goes away
                    *self.local_alerts.lock().await = alerts.clone();
                    return alerts;
                }
            }
        }
        self.local_alerts.lock().await.clone()
    }

    // ── Active / standby ──

    /// Waits until a background service should run on this pod: the next tick
    /// of its interval while this pod is active, or the moment this pod takes
    /// over from the one that was. On the standby it simply keeps waiting.
    pub async fn next_run(&self, interval: &mut Interval) {
        let mut active = self.active.subscribe();
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if *active.borrow() { return; }
                }
                changed = active.changed() => {
                    if changed.is_ok() && *active.borrow_and_update() {
                        // Took over: run now, as the old active pod's last run
                        // may be most of an interval ago, then keep the usual pace
                        interval.reset();
                        return;
                    }
                }
            }
        }
    }

    /// `announce` forces the log line even without a change, for the first
    /// answer after the start (a pod starts out standby, so that one is silent).
    fn set_active(&self, active: bool, why: &str, announce: bool) {
        let changed = self.active.send_if_modified(|current| {
            let changed = *current != active;
            *current = active;
            changed
        });
        if changed || announce {
            if active {
                println!("Cluster: {} is now ACTIVE and runs the background services ({why})", self.pod);
            } else {
                println!("Cluster: {} is now STANDBY; background services wait here ({why})", self.pod);
            }
        }
    }

    /// Keeps trying for the lock, and keeps it renewed while this pod has it.
    /// Also sends this pod's heartbeat over the relay channel.
    async fn hold_lock(self: Arc<Self>) {
        let Some(link) = &self.redis else { return };
        let script  = Script::new(LOCK_LUA);
        let pod_key = format!("{POD_KEY_PREFIX}{}", self.pod);
        let heartbeat = serde_json::to_string(&Envelope {
            origin:   self.pod.clone(),
            seq:      0,
            audience: Audience::everyone(),
            body:     Body::Heartbeat,
        }).unwrap_or_default();
        let (script, pod_key, heartbeat, pod) = (&script, &pod_key, &heartbeat, &self.pod);

        let mut tick = tokio::time::interval(LOCK_TICK);
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        // Since when Redis has not answered about the lock
        let mut unreachable_since: Option<Instant> = None;
        // Until Redis has answered once, so the first answer is always logged
        let mut first_answer = true;
        loop {
            tick.tick().await;
            let held = link.run(|mut conn| async move {
                script.key(LOCK_KEY).key(pod_key).arg(pod).arg(LOCK_TTL.as_millis() as u64)
                    .invoke_async::<i64>(&mut conn).await
            }).await;
            match held {
                Ok(holds) => {
                    unreachable_since = None;
                    if holds == 1 {
                        self.set_active(true, "it holds the lock", first_answer);
                    } else {
                        self.set_active(false, "another pod holds the lock", first_answer);
                    }
                    first_answer = false;
                }
                Err(_) => {
                    // Nobody can say which pod should run the services. A pod
                    // that was active carries on. A standby waits ALONE_AFTER
                    // and then runs them too, as a single pod would, rather
                    // than have nothing run until Redis is fixed. Once Redis
                    // answers again the lock decides, and one of them goes
                    // back to standby.
                    let since = *unreachable_since.get_or_insert_with(Instant::now);
                    if since.elapsed() >= ALONE_AFTER {
                        self.set_active(true, "Redis can't be reached, so it runs them on its own", false);
                    }
                }
            }

            let _ = link.run(|mut conn| async move {
                redis::cmd("PUBLISH").arg(CHANNEL).arg(heartbeat).query_async::<()>(&mut conn).await
            }).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topics(names: &[&str]) -> HashSet<String> {
        names.iter().map(|n| n.to_string()).collect()
    }

    #[test]
    fn everyone_reaches_every_role_on_any_page() {
        let all = Audience::everyone();
        assert!(all.admits("admin", None));
        assert!(all.admits("vaa", Some(&topics(&["io"]))));
    }

    #[test]
    fn topic_needs_a_subscription() {
        let alerts = Audience::topic("part_alerts");
        assert!(alerts.admits("admin", Some(&topics(&["part_alerts", "io"]))));
        assert!(!alerts.admits("admin", Some(&topics(&["io"]))));
        assert!(!alerts.admits("admin", None));
    }

    #[test]
    fn dock_keeps_offsite_roles_to_their_own_dock() {
        assert!(Audience::dock("V").admits("vaa", None));
        assert!(!Audience::dock("A").admits("vaa", None));
        assert!(!Audience::dock("V").admits("univ", None));
        assert!(Audience::dock("A").admits("manager", None));
    }

    #[test]
    fn onsite_only_drops_offsite_roles_even_when_subscribed() {
        let io = Audience::topic("io").onsite_only();
        let subscribed = topics(&["io"]);
        assert!(io.admits("dock", Some(&subscribed)));
        assert!(!io.admits("vaa", Some(&subscribed)));
        assert!(!io.admits("univ", Some(&subscribed)));
    }

    #[test]
    fn topic_and_dock_both_have_to_pass() {
        let add_on = Audience::topic("live_sheet").and_dock("U");
        let subscribed = topics(&["live_sheet"]);
        assert!(add_on.admits("univ", Some(&subscribed)));
        assert!(!add_on.admits("vaa", Some(&subscribed)));
        assert!(!add_on.admits("univ", Some(&topics(&["next_shift"]))));
    }

    #[test]
    fn envelopes_survive_the_trip_through_redis() {
        let sent = Envelope {
            origin:   "pod-a".to_string(),
            seq:      7,
            audience: Audience::topic("io").onsite_only(),
            body:     Body::AddOn { ws_type: "add_on".to_string(), trailer: TrailerRecord { uuid: "u1".to_string(), ..Default::default() } },
        };
        let back: Envelope = serde_json::from_str(&serde_json::to_string(&sent).unwrap()).unwrap();
        assert_eq!(back.origin, "pod-a");
        assert_eq!(back.seq, 7);
        assert_eq!(back.audience, sent.audience);
        assert!(matches!(back.body, Body::AddOn { ws_type, trailer } if ws_type == "add_on" && trailer.uuid == "u1"));
    }
}
