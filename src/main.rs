extern crate rocket;

mod auth;
mod role;
mod structs;
mod getters;
mod loginroutes;
mod setters;
mod wsserver;
mod helpers;
use rocket::data::ToByteUnit;
use rocket::routes;
use neo4rs::Graph;
use structs::{AppState, late_trailer_service, part_monitoring_service};
use tokio::sync::Mutex;
use std::{collections::HashMap, sync::Arc};
use rocket_cors::{CorsOptions, AllowedHeaders};
use rocket::fairing::AdHoc;
use getters::*;
use loginroutes::*;
use setters::*;
use wsserver::*;


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
    pub async fn new() -> Self {
        let graph = Graph::new("bolt://localhost:7687", "neo4j", "Asdf123$").await.unwrap();

        AppState {
            ws_list: Arc::new(Mutex::new(HashMap::new())),
            graph: Arc::new(graph),
            jwt_secret: "tO7E8uCjD5rXpQl0FhKwV2yMz4bJnAi9sGeR3kTzXvNmPuLsDq8W".to_string(),
            alerted_parts: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[rocket::main]
async fn main() {
    dotenvy::dotenv().ok();
    let state = AppState::new().await;


    // Configure CORS
    let cors = custom_cors();

    rocket::custom(
        rocket::Config {
            address: "0.0.0.0".parse().expect("Invalid IP address"),
            port: 8000,
            limits: rocket::data::Limits::new()
                .limit("json", 50.mebibytes()), 
            ..rocket::Config::default()
        }
    )
        .attach(cors)
        .attach(AdHoc::on_liftoff("Start Background Services", |rocket| Box::pin(async move {
            let state = rocket.state::<AppState>().unwrap();
            let ws_list = state.ws_list.clone();
            let graph = state.graph.clone();

            // WebSocket server
            tokio::spawn(async move {
                if let Err(e) = run_ws_server(ws_list.clone()).await {
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
            tokio::spawn(async move {
                part_monitoring_service(graph3, ws_list3, alerted).await;
            });
        })))
        .mount("/", routes![
            roll_next_shift,
            ws_handler,
            refresh_token,
            get_part_info,
            get_edock_asn,
            get_scan_asn,
            get_scan_asn_deck,
            get_scan_decks,
            get_edock_asl,
            get_scan_parts,
            login,
            update_user,
            get_lms,
            get_users_admin,
            get_dy,
            get_trailers_grouped,
            update_user_position,
            get_io,
            get_delivered,
            delete_user,
            upload_on_deck,
            upload_part_asl,
            upload_part_asn,
            upload_part_out,
            update_io,
            update_live_trailer,
            get_staged_trailers,
            get_past_shift,
            get_live_trailers,
            push_add_on,
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
            get_exceptions,
            upload_exception,
            delivered,
            upload_dycomm,
            upload_in_transit,
            register,
            upload_lms,
            push_reschedules
            ])
        .manage(state)
        .launch()
        .await
        .unwrap();
}