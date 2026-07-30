use std::{future::Future, io, net::SocketAddr};

use axum::Router;
use tokio::net::TcpListener;

pub async fn serve_router<F>(listener: TcpListener, router: Router, shutdown: F) -> io::Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    axum::serve(
        listener,
        router.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown)
    .await
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use axum::{Router, routing::get};
    use tokio::sync::{Notify, oneshot};

    use super::*;

    #[tokio::test]
    async fn socket_server_stops_accepting_and_waits_for_in_flight_work() {
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let route_entered = entered.clone();
        let route_release = release.clone();
        let router = Router::new().route(
            "/slow",
            get(move || {
                let entered = route_entered.clone();
                let release = route_release.clone();
                async move {
                    entered.notify_one();
                    release.notified().await;
                    "complete"
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server = tokio::spawn(serve_router(listener, router, async move {
            let _ = shutdown_rx.await;
        }));
        let request = tokio::spawn(async move {
            reqwest::get(format!("http://{address}/slow"))
                .await
                .expect("request")
        });

        entered.notified().await;
        shutdown_tx.send(()).expect("shutdown");
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!server.is_finished());
        release.notify_one();
        assert_eq!(
            request.await.expect("request task").status(),
            reqwest::StatusCode::OK
        );
        assert!(
            tokio::time::timeout(Duration::from_secs(1), server)
                .await
                .expect("server drain timeout")
                .expect("server task")
                .is_ok()
        );
    }
}
