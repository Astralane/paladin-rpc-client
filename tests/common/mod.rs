use once_cell::sync::OnceCell;
use tracing_subscriber::{fmt, EnvFilter};

static TRACING: OnceCell<()> = OnceCell::new();

pub fn init_tracing() {
    TRACING.get_or_init(|| {
        fmt().with_env_filter(EnvFilter::from_default_env()).init();
    });
}
