use tokio::time::{interval, Duration};
use tracing::info;

pub async fn start_tip_stream() {
    let mut interval = interval(Duration::from_millis(100));
    loop {
        interval.tick().await;

        // Placeholder tip info
        let tips = vec![("tx1", 0.001), ("tx2", 0.002)];

        for (tx, tip) in tips.iter() {
            info!(%tx, %tip, "Tip stream update");
        }
    }
}