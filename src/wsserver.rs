use std::net::SocketAddr;
use std::sync::Arc;
use futures_util::{StreamExt, SinkExt};
use rocket::{get, State};
use tokio::net::TcpListener;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_tungstenite::{
    accept_hdr_async, WebSocketStream,
    tungstenite::{
        protocol::Message,
        http::StatusCode,
        handshake::server::{Request as WsRequest, Response as WsResponse},
    },
};
use native_tls::{Identity, TlsAcceptor};
use tokio_native_tls::TlsAcceptor as AsyncTlsAcceptor;
use neo4rs::Graph;
use crate::auth::decode_token;
use crate::structs::{
    AppState, IncomingMessage, WebSocketList, WsTopics,
    TOPIC_PART_ALERTS, TOPIC_LIVE_SHEET, TOPIC_NEXT_SHIFT,
};
use crate::dock_capacity::WS_DOCK_CAPACITY;
use crate::route_blackouts::WS_ROUTE_BLACKOUTS;

#[get("/ws")]
pub async fn ws_handler(state: &State<AppState>) -> Result<(), rocket::http::Status> {
    let ws_list    = state.ws_list.clone();
    let ws_topics  = state.ws_topics.clone();
    let jwt_secret = state.jwt_secret.clone();
    let graph      = state.graph.clone();
    let use_https  = state.use_https;
    tokio::spawn(async move {
        if let Err(e) = run_ws_server(ws_list, ws_topics, jwt_secret, graph, use_https).await {
            println!("Error in WebSocket server: {}", e);
        }
    });
    Ok(())
}

pub async fn run_ws_server(
    ws_list:    WebSocketList,
    ws_topics:  WsTopics,
    jwt_secret: String,
    graph:      Arc<Graph>,
    use_https:  bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let tls_acceptor: Option<AsyncTlsAcceptor> = if use_https {
        let cert     = std::fs::read("cert.pem")?;
        let key      = std::fs::read("key.pem")?;
        let identity = Identity::from_pkcs8(&cert, &key)?;
        Some(AsyncTlsAcceptor::from(TlsAcceptor::builder(identity).build()?))
    } else {
        None
    };

    let port = if use_https { 8443 } else { 9001 };
    let listener = TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
    println!(
        "WebSocket server listening on {}://0.0.0.0:{}",
        if use_https { "wss" } else { "ws" },
        port
    );

    while let Ok((stream, _)) = listener.accept().await {
        let peer_addr = stream.peer_addr().expect("connected streams should have a peer address");
        let secret    = jwt_secret.clone();
        let ws_list   = ws_list.clone();
        let ws_topics = ws_topics.clone();
        let graph     = graph.clone();
        let acceptor  = tls_acceptor.clone();

        tokio::spawn(async move {
            if let Some(acceptor) = acceptor {
                let tls_stream = match acceptor.accept(stream).await {
                    Ok(s)  => s,
                    Err(e) => { println!("TLS handshake failed from {}: {:?}", peer_addr, e); return; }
                };
                match do_ws_handshake(tls_stream, secret).await {
                    Ok((ws, role)) => handle_connection(ws, peer_addr, ws_list, ws_topics, graph, role).await,
                    Err(e) => println!("Connection from {} rejected: {:?}", peer_addr, e),
                }
            } else {
                match do_ws_handshake(stream, secret).await {
                    Ok((ws, role)) => handle_connection(ws, peer_addr, ws_list, ws_topics, graph, role).await,
                    Err(e) => println!("Connection from {} rejected: {:?}", peer_addr, e),
                }
            }
        });
    }

    Ok(())
}

async fn do_ws_handshake<S>(
    stream: S,
    secret: String,
) -> Result<(WebSocketStream<S>, String), tokio_tungstenite::tungstenite::Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let captured_role = Arc::new(std::sync::Mutex::new(String::new()));
    let role_clone    = captured_role.clone();

    let ws = accept_hdr_async(stream, move |req: &WsRequest, res: WsResponse| {
        let token = req.headers()
            .get("Cookie")
            .and_then(|v| v.to_str().ok())
            .and_then(|cookies| {
                cookies.split(';').find_map(|pair| {
                    let pair = pair.trim();
                    pair.strip_prefix("f126f1b7d90a5bd5=").map(|v| v.to_string())
                })
            });

        match token.and_then(|t| decode_token(&t, &secret).ok()) {
            Some(claims) => {
                *role_clone.lock().unwrap() = claims.role;
                Ok(res)
            }
            None => {
                let err = tokio_tungstenite::tungstenite::http::Response::builder()
                    .status(StatusCode::UNAUTHORIZED)
                    .body(None)
                    .unwrap();
                Err(err)
            }
        }
    }).await?;

    let role = captured_role.lock().unwrap().clone();
    Ok((ws, role))
}

async fn handle_connection<S>(
    ws_stream: WebSocketStream<S>,
    peer_addr: SocketAddr,
    ws_list:   WebSocketList,
    ws_topics: WsTopics,
    _graph:    Arc<Graph>,
    role:      String,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (mut ws_sender, mut ws_receiver) = ws_stream.split();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

    {
        let mut list = ws_list.lock().await;
        list.insert(peer_addr, (tx, role));
        println!("Added {} to WebSocket list. Total clients: {}", peer_addr, list.len());
    }

    let ws_list_incoming = ws_list.clone();
    let ws_topics_incoming = ws_topics.clone();
    tokio::spawn(async move {
        while let Some(message) = ws_receiver.next().await {
            match message {
                Ok(msg) => {
                    if msg.is_text() {
                        let msg_text = msg.to_text().unwrap();
                        println!("Received message from {}: {}", peer_addr, msg_text);
                        match serde_json::from_str::<IncomingMessage>(msg_text) {
                            Ok(incoming_message) => {
                                match incoming_message.r#type.as_str() {
                                    "ping" => {
                                        continue;
                                    }
                                    // Server-sent only (set_dock_capacity /
                                    // set_route_blackouts, after a save commits). Relayed
                                    // from a client they would let anyone repaint every
                                    // screen's capacities or blackouts with made-up values.
                                    t if t == WS_DOCK_CAPACITY || t == WS_ROUTE_BLACKOUTS => {
                                        println!("Dropped client-sent {} from {}", t, peer_addr);
                                        continue;
                                    }
                                    // Topic opt-in/out. These are for this server only —
                                    // they carry no payload for other clients, so they are
                                    // never relayed.
                                    "subscribe" | "unsubscribe" => {
                                        let topic = incoming_message.data
                                            .as_ref()
                                            .map(|d| d.message.clone())
                                            .unwrap_or_default();
                                        if topic.is_empty() {
                                            println!("Ignoring {} with no topic from {}", incoming_message.r#type, peer_addr);
                                            continue;
                                        }
                                        let mut topics = ws_topics_incoming.lock().await;
                                        if incoming_message.r#type == "subscribe" {
                                            topics.entry(peer_addr).or_default().insert(topic.clone());
                                            println!("{} subscribed to {}", peer_addr, topic);
                                        } else if let Some(set) = topics.get_mut(&peer_addr) {
                                            set.remove(&topic);
                                            println!("{} unsubscribed from {}", peer_addr, topic);
                                        }
                                        continue;
                                    }
                                    "trailer_update" => {
                                        println!("Handling trailer_update: {:?}", incoming_message.data);
                                    }
                                    "add_on" | "staged_add_on" => {
                                        println!("Handling {}: {:?}", incoming_message.r#type, incoming_message.data);
                                    }
                                    "part_alert" => {
                                        println!("Handling part_alert: {:?}", incoming_message.data);
                                    }
                                    "multi_trailer_update" => {
                                        println!("Handling multi_trailer_update: {:?}", incoming_message.data);
                                    }
                                    // Schedule builder state sync. The payload carries the whole
                                    // in-progress schedule, so it is relayed unlogged; only the
                                    // clients on that page act on it.
                                    "schedule_broadcast" => {
                                        println!("Handling schedule_broadcast from {}", peer_addr);
                                    }
                                    _ => {
                                        println!("Unknown event type: {:?}", incoming_message.r#type);
                                    }
                                }

                                // Opt-in feeds reach subscribers only; everything else
                                // still relays to every client.
                                let required_topic = match incoming_message.r#type.as_str() {
                                    "part_alert"    => Some(TOPIC_PART_ALERTS),
                                    "add_on"        => Some(TOPIC_LIVE_SHEET),
                                    "staged_add_on" => Some(TOPIC_NEXT_SHIFT),
                                    _               => None,
                                };

                                let response = Message::Text(serde_json::to_string(&incoming_message).unwrap());
                                let list = ws_list_incoming.lock().await;
                                let topics = ws_topics_incoming.lock().await;
                                println!("Broadcasting message to {} clients", list.len());
                                for (addr, (sender, _role)) in list.iter() {
                                    if let Some(topic) = required_topic {
                                        if !topics.get(addr).is_some_and(|t| t.contains(topic)) {
                                            continue;
                                        }
                                    }
                                    if sender.send(response.clone()).is_err() {
                                        println!("Failed to send message to {}", peer_addr);
                                    }
                                }
                            }
                            Err(e) => {
                                println!("Failed to parse incoming message: {:?}", e);
                            }
                        }
                    } else if msg.is_binary() {
                        println!("Received binary message from {}", peer_addr);
                    } else if msg.is_close() {
                        println!("Received close message from {}", peer_addr);
                        break;
                    }
                }
                Err(e) => {
                    println!("WebSocket error with {}: {}", peer_addr, e);
                    break;
                }
            }
        }
        let mut list = ws_list_incoming.lock().await;
        list.remove(&peer_addr);
        ws_topics_incoming.lock().await.remove(&peer_addr);
        println!("Client {} removed. Total clients: {}", peer_addr, list.len());
    });

    let ws_list_outgoing = ws_list.clone();
    let ws_topics_outgoing = ws_topics.clone();
    tokio::spawn(async move {
        while let Some(message) = rx.recv().await {
            if ws_sender.send(message).await.is_err() {
                println!("Failed to send outgoing message to {}", peer_addr);
                break;
            }
        }
        let mut list = ws_list_outgoing.lock().await;
        list.remove(&peer_addr);
        ws_topics_outgoing.lock().await.remove(&peer_addr);
        println!("Client {} disconnected. Total clients: {}", peer_addr, list.len());
    });
}
