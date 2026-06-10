use std::net::SocketAddr;
use std::sync::Arc;
use futures_util::{StreamExt, SinkExt};
use rocket::{get, State};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{
    accept_hdr_async, WebSocketStream,
    tungstenite::{
        protocol::Message,
        http::StatusCode,
        handshake::server::{Request as WsRequest, Response as WsResponse},
    },
};
use neo4rs::Graph;
use crate::auth::decode_token;
use crate::structs::{AppState, IncomingMessage, WebSocketList};

#[get("/ws")]
pub async fn ws_handler(state: &State<AppState>) -> Result<(), rocket::http::Status> {
    let ws_list    = state.ws_list.clone();
    let jwt_secret = state.jwt_secret.clone();
    let graph      = state.graph.clone();
    tokio::spawn(async move {
        if let Err(e) = run_ws_server(ws_list, jwt_secret, graph).await {
            println!("Error in WebSocket server: {}", e);
        }
    });
    Ok(())
}

pub async fn run_ws_server(
    ws_list: WebSocketList,
    jwt_secret: String,
    graph: Arc<Graph>,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("0.0.0.0:9001").await?;
    println!("WebSocket server listening on ws://0.0.0.0:9001");

    while let Ok((stream, _)) = listener.accept().await {
        let peer_addr = stream.peer_addr().expect("connected streams should have a peer address");
        let secret = jwt_secret.clone();

        let ws_stream = match accept_hdr_async(stream, move |req: &WsRequest, res: WsResponse| {
            let token = req.headers()
                .get("Cookie")
                .and_then(|v| v.to_str().ok())
                .and_then(|cookies| {
                    cookies.split(';').find_map(|pair| {
                        let pair = pair.trim();
                        pair.strip_prefix("access_token=").map(|v| v.to_string())
                    })
                });

            match token.filter(|t| decode_token(t, &secret).is_ok()) {
                Some(_) => Ok(res),
                None => {
                    let err = tokio_tungstenite::tungstenite::http::Response::builder()
                        .status(StatusCode::UNAUTHORIZED)
                        .body(None)
                        .unwrap();
                    Err(err)
                }
            }
        }).await {
            Ok(ws) => ws,
            Err(e) => {
                println!("Connection from {} rejected: {:?}", peer_addr, e);
                continue;
            }
        };

        println!("WebSocket connection accepted from {}", peer_addr);
        tokio::spawn(handle_connection(
            ws_stream,
            peer_addr,
            ws_list.clone(),
            graph.clone(),
        ));
    }

    Ok(())
}

async fn send_to_peer(ws_list: &WebSocketList, peer_addr: SocketAddr, msg: IncomingMessage) {
    if let Ok(text) = serde_json::to_string(&msg) {
        let list = ws_list.lock().await;
        if let Some(sender) = list.get(&peer_addr) {
            let _ = sender.send(Message::Text(text));
        }
    }
}

async fn handle_connection(
    ws_stream: WebSocketStream<TcpStream>,
    peer_addr: SocketAddr,
    ws_list: WebSocketList,
    _graph: Arc<Graph>,
) {
    let (mut ws_sender, mut ws_receiver) = ws_stream.split();

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

    {
        let mut list = ws_list.lock().await;
        list.insert(peer_addr, tx);
        println!("Added {} to WebSocket list. Total clients: {}", peer_addr, list.len());
    }

    let ws_list_incoming = ws_list.clone();
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
                                        // Keep-alive only — auth is enforced at connect time.
                                        continue;
                                    }
                                    "trailer_update" => {
                                        println!("Handling trailer_update: {:?}", incoming_message.data);
                                    }
                                    "add_on" => {
                                        println!("Handling add_on: {:?}", incoming_message.data);
                                    }
                                    "part_alert" => {
                                        println!("Handling part_alert: {:?}", incoming_message.data);
                                    }
                                    "multi_trailer_update" => {
                                        println!("Handling multi_trailer_update: {:?}", incoming_message.data);
                                    }
                                    _ => {
                                        println!("Unknown event type: {:?}", incoming_message.r#type);
                                    }
                                }

                                let response = Message::Text(serde_json::to_string(&incoming_message).unwrap());
                                let list = ws_list_incoming.lock().await;
                                println!("Broadcasting message to {} clients", list.len());
                                for sender in list.values() {
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
        println!("Client {} removed. Total clients: {}", peer_addr, list.len());
    });

    let ws_list_outgoing = ws_list.clone();
    tokio::spawn(async move {
        while let Some(message) = rx.recv().await {
            if ws_sender.send(message).await.is_err() {
                println!("Failed to send outgoing message to {}", peer_addr);
                break;
            }
        }
        let mut list = ws_list_outgoing.lock().await;
        list.remove(&peer_addr);
        println!("Client {} disconnected. Total clients: {}", peer_addr, list.len());
    });
}
