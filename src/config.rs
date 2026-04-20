//! Application configuration loaded from environment variables.

use std::env;

#[derive(Debug, Clone)]
pub struct AppConfig {
    /// The AI model to use (e.g. "gemma-4-31b-it")
    pub ai_model: String,
    /// Server port
    pub port: u16,
    /// Google API key (for Gemma 4)
    pub google_api_key: String,
    /// YouTube Data API v3 key (separate from Gemma key)
    pub youtube_api_key: String,
    /// Tavily API key (for web search)
    pub tavily_api_key: String,
    /// Telegram Bot token
    pub telegram_bot_token: String,
    /// Paystack secret key
    pub _paystack_secret_key: String,
    /// Supabase URL
    pub _supabase_url: String,
    /// Supabase anon/service key
    pub _supabase_key: String,
    /// Webhook URL — if set, use webhook mode; if absent, use long-polling
    pub webhook_url: Option<String>,
    /// Directory to store feedback JSON files
    pub feedback_dir: String,
    /// Admin API key for protected endpoints
    pub admin_api_key: String,
    /// Whether the bot is in testing mode (all tiers free)
    pub testing_mode: bool,
    /// Maximum research requests per chat per hour
    pub max_requests_per_hour: usize,
    /// Bridge authentication secret
    pub bridge_secret: String,
    /// Allowed users via WhatsApp (None = public, open to everyone)
    pub allowed_users: Option<Vec<i64>>,
}

impl AppConfig {
    pub fn from_env() -> Self {
        Self {
            ai_model: env::var("AI_MODEL").unwrap_or_else(|_| "gemma-4-31b-it".to_string()),
            port: env::var("PORT")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(3000),
            google_api_key: env::var("GOOGLE_API_KEY").unwrap_or_default(),
            youtube_api_key: env::var("YOUTUBE_API_KEY").unwrap_or_default(),
            tavily_api_key: env::var("TAVILY_API_KEY").unwrap_or_default(),
            telegram_bot_token: env::var("TELEGRAM_BOT_TOKEN").unwrap_or_default(),
            _paystack_secret_key: env::var("PAYSTACK_SECRET_KEY").unwrap_or_default(),
            _supabase_url: env::var("SUPABASE_URL").unwrap_or_default(),
            _supabase_key: env::var("SUPABASE_KEY").unwrap_or_default(),
            webhook_url: env::var("WEBHOOK_URL").ok().filter(|v| !v.is_empty()),
            feedback_dir: env::var("FEEDBACK_DIR").unwrap_or_else(|_| "./feedback".to_string()),
            admin_api_key: env::var("ADMIN_API_KEY")
                .unwrap_or_else(|_| "professor-ai-admin-2026".to_string()),
            testing_mode: env::var("TESTING_MODE")
                .map(|v| v != "false" && v != "0")
                .unwrap_or(true),
            max_requests_per_hour: env::var("MAX_REQUESTS_PER_HOUR")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(3),
            bridge_secret: env::var("BRIDGE_SECRET")
                .unwrap_or_else(|_| "local_dev_secret_123".to_string()),
            allowed_users: env::var("ALLOWED_USERS")
                .ok()
                .filter(|v| !v.is_empty())
                .map(|v| {
                    v.split(',')
                        .filter_map(|s| s.trim().parse::<i64>().ok())
                        .collect()
                }),
        }
    }
}
