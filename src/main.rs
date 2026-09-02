extern crate rocket;

mod auth;
mod role;
mod structs;
mod getters;
mod loginroutes;
mod setters;
mod wsserver;
mod helpers;
mod emailer;
mod services;
mod ldap_auth;
use rocket::data::ToByteUnit;
use rocket::{get, routes};
use rocket::fs::{FileServer, NamedFile};
use rocket::config::TlsConfig;
use neo4rs::Graph;
use structs::AppState;
use services::{late_trailer_service, part_monitoring_service};
use tokio::sync::Mutex;
use std::{collections::HashMap, sync::Arc};
use rocket_cors::{CorsOptions, AllowedHeaders};
use rocket::fairing::AdHoc;
use getters::*;
use loginroutes::*;
use setters::*;
use wsserver::*;
use emailer::*;


/*
    CORS Config
*/

fn custom_cors() -> rocket_cors::Cors {
    CorsOptions::default()
        .allowed_origins(rocket_cors::AllOrSome::All)
        .allowed_headers(AllowedHeaders::some(&["Authorization", "Accept", "Content-Type"]))
        .allow_credentials(true)
        .to_cors()
        .expect("error creating CORS fairing")
}

impl AppState {
    pub async fn new(use_https: bool) -> Self {
        let neo4j_uri  = std::env::var("NEO4J_URI").unwrap_or_else(|_| "bolt://localhost:7687".to_string());
        let neo4j_user = std::env::var("NEO4J_USER").unwrap_or_else(|_| "neo4j".to_string());
        let neo4j_pass = std::env::var("NEO4J_PASSWORD").expect("NEO4J_PASSWORD must be set");
        let graph = Graph::new(&neo4j_uri, &neo4j_user, &neo4j_pass).await.unwrap();

        let jwt_secret = std::env::var("JWT_SECRET").expect("JWT_SECRET must be set");

        AppState {
            ws_list: Arc::new(Mutex::new(HashMap::new())),
            graph: Arc::new(graph),
            jwt_secret,
            alerted_parts: Arc::new(Mutex::new(HashMap::new())),
            edit_refs:     Arc::new(Mutex::new(HashMap::new())),
            current_alerts: Arc::new(Mutex::new(Vec::new())),
            use_https,
        }
    }
}

#[get("/<_..>", rank = 20)]
async fn spa_fallback() -> Option<NamedFile> {
    NamedFile::open("dist/index.html").await.ok()
}

#[rocket::main]
async fn main() {
    dotenvy::dotenv().ok();
    let use_https = std::env::args().any(|a| a == "--https");
    let state = AppState::new(use_https).await;

    let cors = custom_cors();

    let rocket_config = if use_https {
        rocket::Config {
            address: "0.0.0.0".parse().expect("Invalid IP address"),
            port: 8443,
            tls: Some(TlsConfig::from_paths("cert.pem", "key.pem")),
            limits: rocket::data::Limits::new().limit("json", 50.mebibytes()),
            ..rocket::Config::default()
        }
    } else {
        rocket::Config {
            address: "0.0.0.0".parse().expect("Invalid IP address"),
            port: 8000,
            limits: rocket::data::Limits::new().limit("json", 50.mebibytes()),
            ..rocket::Config::default()
        }
    };

    rocket::custom(rocket_config)
        .attach(cors)
        .attach(AdHoc::on_liftoff("Start Background Services", |rocket| Box::pin(async move {
            let state = rocket.state::<AppState>().unwrap();
            let ws_list = state.ws_list.clone();
            let graph = state.graph.clone();
            let jwt_secret  = state.jwt_secret.clone();
            let ws_graph    = state.graph.clone();
            let use_https   = state.use_https;

            // WebSocket server
            tokio::spawn(async move {
                if let Err(e) = run_ws_server(ws_list.clone(), jwt_secret, ws_graph, use_https).await {
                    println!("Error in WebSocket server: {}", e);
                }
            });

            // Late trailer service
            let ws_list2 = state.ws_list.clone();
            tokio::spawn(async move {
                late_trailer_service(graph, ws_list2).await;
            });

            let graph3  = state.graph.clone();
            let ws_list3 = state.ws_list.clone();
            let alerted = state.alerted_parts.clone();
            let current_alerts = state.current_alerts.clone();
            tokio::spawn(async move {
                part_monitoring_service(graph3, ws_list3, alerted, current_alerts).await;
            });
        })))
        .mount("/", FileServer::from("dist"))
        .mount("/", routes![
            roll_next_shift,
            ws_handler,
            refresh_token,
            get_edock_asn,
            get_part_asn,
            get_part_asl,
            get_scan_asn,
            get_scan_asn_deck,
            get_scan_decks,
            get_deck_assignee_slack,
            send_slack,
            send_rescheduled_slack,
            get_edock_asl,
            get_scan_parts,
            login,
            sso_login,
            update_user,
            get_lms,
            get_lms_by_route,
            get_lms_by_load,
            get_users_admin,
            get_contacts,
            get_part_alerts,
            get_carriers,
            get_route_contacts,
            get_route_carrier_contacts,
            update_contact,
            delete_contact,
            get_dy,
            get_trailers_grouped,
            update_user_position,
            get_io,
            get_delivered,
            delete_user,
            upload_on_deck,
            upload_part_asl,
            upload_part_asn,
            upload_part_route,
            upload_part_out,
            update_io,
            update_live_trailer,
            get_staged_trailers,
            get_past_shift,
            get_live_trailers,
            push_add_on,
            get_hot_parts,
            get_dock_count,
            restart_week,
            saturday_counts,
            set_shift_status,
            get_users,
            unassign_deck,
            generate_week,
            assign_deck,
            get_shift_detail,
            get_decks,
            get_week,
            assign_shift,
            unassign_shift,
            get_scan_routes,
            get_part_routes,
            get_exceptions,
            upload_exception,
            delivered,
            upload_dycomm,
            upload_in_transit,
            upload_lms,
            push_reschedules,
            create_hot_part,
            close_hot_part,
            send_email_route,
            logout,
            get_audit_events,
            spa_fallback
            ])
        .manage(state)
        .launch()
        .await
        .unwrap();
}