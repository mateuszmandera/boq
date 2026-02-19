use anyhow::Result;
use std::sync::Arc;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::watch;

#[derive(Clone)]
pub struct Sender {
    tx: Arc<watch::Sender<bool>>,
}

#[derive(Clone)]
pub struct Receiver {
    rx: watch::Receiver<bool>,
}

pub fn channel() -> Result<(Sender, Receiver)> {
    let mut interrupt = signal(SignalKind::interrupt())?;
    let mut terminate = signal(SignalKind::terminate())?;
    let (tx, rx) = watch::channel(false);
    let sender = Sender { tx: Arc::new(tx) };
    tokio::spawn({
        let sender = sender.clone();
        async move {
            tokio::select! {
                _ = interrupt.recv() => (),
                _ = terminate.recv() => (),
            }
            sender.shutdown();
        }
    });
    Ok((sender, Receiver { rx }))
}

impl Sender {
    pub fn shutdown(&self) {
        _ = self.tx.send_if_modified(|sent| {
            !*sent && {
                tracing::info!("shutting down");
                *sent = true;
                true
            }
        });
    }

    pub fn on_error<T, E, U>(&self, fut: U) -> impl Future<Output = Result<T, E>> + use<T, E, U>
    where
        U: Future<Output = Result<T, E>>,
    {
        let sender = self.clone();
        async move {
            fut.await.map_err(|error| {
                sender.shutdown();
                error
            })
        }
    }
}

impl Receiver {
    pub async fn wait(&mut self) {
        if !*self.rx.borrow_and_update() {
            _ = self.rx.changed().await;
        }
    }
}
