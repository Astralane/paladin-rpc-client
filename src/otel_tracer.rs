use opentelemetry::trace::TracerProvider;
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::{LogExporter, WithExportConfig};
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::Resource;
use serde::Deserialize;
use tracing::subscriber::set_global_default;
use tracing::{error, Subscriber};
use tracing_log::LogTracer;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::{fmt, EnvFilter, Registry};

pub async fn get_subscriber_with_otpl<Sink>(
    env_filter: String,
    jaeger_endpoint: String,
    sink: Sink,
) -> impl Subscriber + Send + Sync
where
    Sink: for<'a> MakeWriter<'a> + Send + Sync + 'static,
{
    error!("-------------------{}",jaeger_endpoint);
    let service_name = format!(
        "paladin-blockengin",
    );
    let tracer = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(
            opentelemetry_otlp::SpanExporter::builder()
                .with_tonic()
                .with_endpoint(jaeger_endpoint.clone())
                .build()
                .expect("Couldn't create OTLP tracer"),
        )
        .with_resource(
            Resource::builder()
                .with_service_name(service_name.clone())
                .build(),
        )
        .build()
        .tracer("iris");

    let telemetry_layer: tracing_opentelemetry::OpenTelemetryLayer<
        Registry,
        opentelemetry_sdk::trace::Tracer,
    > = tracing_opentelemetry::layer().with_tracer(tracer);

    let log_tracer = SdkLoggerProvider::builder()
        .with_batch_exporter(
            LogExporter::builder()
                .with_tonic()
                .with_endpoint(jaeger_endpoint)
                .build()
                .expect("Couldn't create OTL tracer"),
        )
        .with_resource(Resource::builder().with_service_name(service_name).build())
        .build();

    let logging_layer = OpenTelemetryTracingBridge::new(&log_tracer);

    let env_filter = EnvFilter::new(env_filter);
    let format_layer = fmt::Layer::default().with_writer(sink);

    Registry::default()
        .with(telemetry_layer)
        .with(logging_layer)
        .with(env_filter)
        .with(format_layer)
}

pub fn init_subscriber(subscriber: impl Subscriber + Send + Sync) {
    LogTracer::init().expect("Failed to set log filter");
    set_global_default(subscriber).expect("Failed to set subscriber");
}

pub fn init_subscriber_without_signoz<Sink>(sink: Sink)
where
    Sink: for<'a> MakeWriter<'a> + Send + Sync + 'static,
{
    let env_filter = EnvFilter::try_from_default_env().unwrap_or(EnvFilter::new("info"));
    let format_layer = fmt::Layer::default().with_writer(sink);
    let subscriber = Registry::default().with(format_layer).with(env_filter);
    LogTracer::init().expect("Failed to set log filter");
    set_global_default(subscriber).expect("Failed to set subscriber");
}