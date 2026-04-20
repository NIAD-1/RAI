//! Research Bot Server — Telegram Academic Research Assistant
//!
//! A lightweight Axum server that bridges Telegram messages to the Claw engine
//! for autonomous academic research, project writing, and citation verification.

mod config;
mod models;
mod routes;
mod services;

use axum::{
    routing::{get, post},
    Router,
};
use std::sync::Arc;
use tower_http::cors::CorsLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::config::AppConfig;
use crate::services::AppState;

#[tokio::main]
async fn main() {
    // Initialize logging
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "research_bot=info,tower_http=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Load config from environment
    dotenvy::dotenv().ok();
    let config = AppConfig::from_env();

    tracing::info!("🎓 Research Bot Server starting...");
    tracing::info!("  Model: {}", config.ai_model);
    tracing::info!("  Port:  {}", config.port);
    tracing::info!(
        "  Mode:  {}",
        if config.testing_mode {
            "🧪 BETA TESTING (all tiers free)"
        } else {
            "💰 PRODUCTION (paid tiers)"
        }
    );
    tracing::info!(
        "  Telegram: {}",
        if config.webhook_url.is_some() {
            "webhook mode"
        } else {
            "long-polling mode"
        }
    );
    tracing::info!("  Feedback dir: {}", config.feedback_dir);
    tracing::info!(
        "  Rate limit: {} requests/hour",
        config.max_requests_per_hour
    );

    let port = config.port;
    let webhook_url = config.webhook_url.clone();
    let telegram_token = config.telegram_bot_token.clone();

    let state = Arc::new(AppState::new(config));

    let app = Router::new()
        // Health check
        .route("/health", get(routes::health_check))
        // WhatsApp bridge endpoints
        .route("/bridge/incoming", post(routes::whatsapp_bridge_incoming))
        .route("/bridge/audio", post(routes::whatsapp_bridge_audio))
        // Telegram webhook
        .route("/webhook/telegram", post(routes::telegram_webhook))
        // Paystack webhook (payment confirmation)
        .route("/webhook/paystack", post(routes::paystack_webhook))
        // Admin feedback dashboard
        .route("/admin/feedback", get(routes::admin_feedback))
        // Simulation endpoint (for local testing without Telegram)
        .route("/simulate", post(routes::simulate_research))
        // QR code page for WhatsApp linking
        .route("/qr", get(routes::qr_page))
        .layer(CorsLayer::permissive())
        .with_state(state.clone());

    // Set up Telegram only when a bot token exists.
    if !telegram_token.trim().is_empty() {
        if let Some(ref url) = webhook_url {
            let webhook_endpoint = format!("{url}/webhook/telegram");
            match services::set_telegram_webhook(&telegram_token, &webhook_endpoint).await {
                Ok(()) => tracing::info!("✅ Telegram webhook registered: {webhook_endpoint}"),
                Err(error) => tracing::error!("❌ Failed to set Telegram webhook: {error}"),
            }
        } else {
            let polling_state = state.clone();
            tokio::spawn(async move {
                services::start_telegram_polling(polling_state).await;
            });
        }
    } else {
        tracing::info!("📵 Telegram disabled — TELEGRAM_BOT_TOKEN not set.");
    }

    let addr = format!("0.0.0.0:{}", port);
    tracing::info!("🚀 Listening on {addr}");

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
