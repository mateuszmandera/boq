use anyhow::Result;
use axum::Router;
use axum::middleware;
use axum::routing::{get, post};
use hyper_util::rt::TokioIo;
use std::net::SocketAddr;
use std::pin::pin;
use std::sync::Arc;
use tokio::net::TcpListener;
use tower_service::Service;

use crate::app_state::AppState;
use crate::auth::{api_auth, django_session_auth};
use crate::debug::print_request_response;
use crate::handlers;
use crate::shutdown;

pub struct AppServer {
    app: Router,
    listener: TcpListener,
    shutdown_rx: shutdown::Receiver,
}

impl AppServer {
    pub async fn new(address: &SocketAddr, state: Arc<AppState>) -> Result<AppServer> {
        let shutdown_rx = state.shutdown_rx.clone();

        let app = Router::new()
            .route("/", get(handlers::get_root))
            .route("/notify_tornado", post(handlers::post_notify_tornado))
            .route(
                "/json/events",
                get(handlers::get_events)
                    .delete(handlers::delete_events)
                    .route_layer(middleware::map_request_with_state(
                        state.clone(),
                        django_session_auth,
                    )),
            )
            .route(
                "/api/v1/events",
                get(handlers::get_events)
                    .delete(handlers::delete_events)
                    .route_layer(middleware::map_request_with_state(state.clone(), api_auth)),
            )
            .route(
                "/api/v1/events/internal",
                post(handlers::post_events_internal),
            )
            .with_state(state)
            .layer(middleware::from_fn(print_request_response));

        let listener = TcpListener::bind(address).await?;
        tracing::info!("listening on {address}", address = listener.local_addr()?);
        Ok(AppServer {
            app,
            listener,
            shutdown_rx,
        })
    }

    pub async fn run(mut self) -> Result<()> {
        loop {
            let (socket, _remote_addr) = tokio::select! {
                 result = self.listener.accept() => result?,
                 () = self.shutdown_rx.wait() => return Ok(()),
            };
            let app = self.app.clone();
            let mut shutdown_rx = self.shutdown_rx.clone();
            tokio::spawn(async move {
                let socket = TokioIo::new(socket);
                let hyper_service =
                    hyper::service::service_fn(move |request| app.clone().call(request));
                let conn = hyper::server::conn::http1::Builder::new()
                    .serve_connection(socket, hyper_service)
                    .with_upgrades();
                let mut conn = pin!(conn);
                loop {
                    tokio::select! {
                        result = conn.as_mut() => {
                            if let Err(err) = result {
                                tracing::debug!("failed to serve connection: {err:#}");
                            }
                            break;
                        }
                        () = shutdown_rx.wait() => conn.as_mut().graceful_shutdown(),
                    }
                }
            });
        }
    }
}
