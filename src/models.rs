//! Data models for Telegram, Paystack, feedback, tiers, and generated research reports.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// ── Telegram Bot API Models ───────────────────────────────────────

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct TelegramUpdate {
    #[serde(rename = "update_id")]
    pub update_id: u64,
    pub message: Option<TelegramMessage>,
    pub callback_query: Option<TelegramCallbackQuery>,
}

#[derive(Debug, Deserialize)]
pub struct TelegramMessage {
    #[serde(rename = "message_id")]
    pub _message_id: u64,
    #[serde(rename = "from")]
    pub from: Option<TelegramUser>,
    pub chat: TelegramChat,
    pub text: Option<String>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct TelegramUser {
    pub id: u64,
    #[serde(rename = "first_name")]
    pub first_name: String,
    #[serde(rename = "username")]
    pub username: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TelegramChat {
    pub id: i64,
}

#[derive(Debug, Deserialize)]
pub struct TelegramCallbackQuery {
    pub id: String,
    pub from: TelegramUser,
    pub message: Option<TelegramCallbackMessage>,
    pub data: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TelegramCallbackMessage {
    pub chat: TelegramChat,
    #[serde(rename = "message_id")]
    pub _message_id: u64,
}

// ── Paystack Webhook Models ───────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct PaystackWebhookPayload {
    pub event: String,
    pub data: PaystackData,
}

#[derive(Debug, Deserialize)]
pub struct PaystackData {
    #[serde(rename = "reference")]
    pub _reference: Option<String>,
    pub amount: Option<u64>,
    #[serde(rename = "currency")]
    pub _currency: Option<String>,
    #[serde(rename = "status")]
    pub _status: Option<String>,
    #[serde(rename = "customer")]
    pub _customer: Option<PaystackCustomer>,
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
pub struct PaystackCustomer {
    #[serde(rename = "email")]
    pub _email: Option<String>,
    #[serde(rename = "phone")]
    pub _phone: Option<String>,
}

// ── Research Tier System (Progressive Unlock) ─────────────────────

/// Tier of research service — progressive unlock model.
///
/// Each tier unlocks more sections of the same report.
/// Users receive upgrade credit: e.g. Starter (₦1,500) → Standard costs only ₦3,500.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ResearchTier {
    /// Free — Abstract + Table of Contents (hook)
    Preview,
    /// ₦1,500 — + Introduction + Literature Review
    Starter,
    /// ₦5,000 — + Methodology + Findings/Analysis
    Standard,
    /// ₦10,000 — Full paper with References + PDF
    Complete,
}

impl ResearchTier {
    pub fn from_amount_kobo(amount: u64) -> Self {
        match amount {
            0 => Self::Preview,
            1..=200_000 => Self::Starter,        // ≤ ₦2,000
            200_001..=700_000 => Self::Standard, // ≤ ₦7,000
            _ => Self::Complete,                 // > ₦7,000
        }
    }

    #[allow(dead_code)]
    pub fn price_kobo(&self) -> u64 {
        match self {
            Self::Preview => 0,
            Self::Starter => 150_000,    // ₦1,500
            Self::Standard => 500_000,   // ₦5,000
            Self::Complete => 1_000_000, // ₦10,000
        }
    }

    #[allow(dead_code)]
    pub fn price_naira(&self) -> u64 {
        self.price_kobo() / 100
    }

    #[allow(dead_code)]
    pub fn upgrade_cost_from(&self, current: &ResearchTier) -> u64 {
        let target_price = self.price_kobo();
        let current_price = current.price_kobo();
        target_price.saturating_sub(current_price)
    }

    pub fn citation_target(&self) -> usize {
        match self {
            Self::Preview => 3,
            Self::Starter => 5,
            Self::Standard => 10,
            Self::Complete => 25,
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::Preview => "Preview (Free)",
            Self::Starter => "Starter",
            Self::Standard => "Standard",
            Self::Complete => "Complete",
        }
    }

    pub fn deliverable_name(&self) -> &'static str {
        match self {
            Self::Preview => "abstract and table of contents",
            Self::Starter => "abstract, TOC, introduction, and literature review",
            Self::Standard => "abstract through methodology and findings",
            Self::Complete => "full academic paper",
        }
    }

    pub fn minimum_word_count(&self) -> usize {
        match self {
            Self::Preview => 500,
            Self::Starter => 2_000,
            Self::Standard => 5_000,
            Self::Complete => 9_000,
        }
    }

    #[allow(dead_code)]
    /// Returns which section headings this tier unlocks.
    pub fn unlocked_sections(&self) -> Vec<&'static str> {
        match self {
            Self::Preview => vec!["Abstract", "Table of Contents"],
            Self::Starter => vec![
                "Abstract",
                "Table of Contents",
                "Introduction",
                "Literature Review",
            ],
            Self::Standard => vec![
                "Abstract",
                "Table of Contents",
                "Introduction",
                "Literature Review",
                "Methodology",
                "Discussion",
            ],
            Self::Complete => vec![
                "Abstract",
                "Table of Contents",
                "Introduction",
                "Literature Review",
                "Methodology",
                "Discussion",
                "Conclusion & Recommendations",
                "References",
            ],
        }
    }

    #[allow(dead_code)]
    pub fn emoji(&self) -> &'static str {
        match self {
            Self::Preview => "📋",
            Self::Starter => "📝",
            Self::Standard => "📊",
            Self::Complete => "📚",
        }
    }

    #[allow(dead_code)]
    /// All tiers in upgrade order.
    pub fn all() -> Vec<ResearchTier> {
        vec![Self::Preview, Self::Starter, Self::Standard, Self::Complete]
    }

    #[allow(dead_code)]
    pub fn next_tier(&self) -> Option<ResearchTier> {
        match self {
            Self::Preview => Some(Self::Starter),
            Self::Starter => Some(Self::Standard),
            Self::Standard => Some(Self::Complete),
            Self::Complete => None,
        }
    }
}

// ── Reference Style ───────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum ReferenceStyle {
    #[default]
    Apa,
    Mla,
    Chicago,
    Harvard,
}

impl ReferenceStyle {
    pub fn from_user_input(input: &str) -> Option<Self> {
        let normalized = input.trim().to_lowercase();
        match normalized.as_str() {
            "apa" | "apa7" | "apa 7" | "apa7th" | "apa 7th" | "apa 7th edition" => Some(Self::Apa),
            "mla" | "mla9" | "mla 9" | "mla9th" | "mla 9th" | "mla 9th edition" => Some(Self::Mla),
            "chicago" | "chicago author-date" | "chicago author date" => Some(Self::Chicago),
            "harvard" | "harvard style" => Some(Self::Harvard),
            _ => None,
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::Apa => "APA 7th edition",
            Self::Mla => "MLA 9th edition",
            Self::Chicago => "Chicago author-date",
            Self::Harvard => "Harvard style",
        }
    }

    pub fn bibliography_heading(&self) -> &'static str {
        match self {
            Self::Mla => "Works Cited",
            Self::Chicago => "Bibliography",
            Self::Apa | Self::Harvard => "References",
        }
    }
}

// ── Feedback System ───────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum FeedbackRating {
    Excellent,
    Good,
    NeedsWork,
    Poor,
}

impl FeedbackRating {
    pub fn from_callback_data(data: &str) -> Option<Self> {
        match data {
            "rate_excellent" => Some(Self::Excellent),
            "rate_good" => Some(Self::Good),
            "rate_needs_work" => Some(Self::NeedsWork),
            "rate_poor" => Some(Self::Poor),
            _ => None,
        }
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::Excellent => "⭐ Excellent",
            Self::Good => "👍 Good",
            Self::NeedsWork => "🔧 Needs Work",
            Self::Poor => "👎 Poor",
        }
    }

    #[allow(dead_code)]
    pub fn callback_data(&self) -> &'static str {
        match self {
            Self::Excellent => "rate_excellent",
            Self::Good => "rate_good",
            Self::NeedsWork => "rate_needs_work",
            Self::Poor => "rate_poor",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserFeedback {
    pub chat_id: i64,
    pub username: Option<String>,
    pub topic: String,
    pub rating: FeedbackRating,
    pub comment: Option<String>,
    pub timestamp: DateTime<Utc>,
}

// ── Conversation State Machine ────────────────────────────────────

#[derive(Debug, Clone)]
pub enum ConversationState {
    /// Waiting for a research topic.
    Idle,
    /// Research is in progress for this topic.
    Researching { topic: String },
    /// Report delivered, awaiting rating via inline keyboard.
    AwaitingFeedback { topic: String },
    /// Rating received, awaiting optional free-text comment.
    AwaitingComment {
        topic: String,
        rating: FeedbackRating,
    },
}

// ── Research Pipeline Models ──────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResearchSource {
    pub source_type: String,
    pub title: String,
    pub author_or_channel: String,
    pub year_hint: Option<i32>,
    pub url: String,
    pub summary: String,
    pub citation_hint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResearchPlan {
    pub title: String,
    pub thesis: String,
    pub methodology: String,
    pub research_questions: Vec<String>,
    pub keywords: Vec<String>,
    pub writing_guidance: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResearchSection {
    pub heading: String,
    pub target_words: usize,
    pub actual_words: usize,
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResearchQualityReport {
    pub required_headings_present: bool,
    pub references_present: bool,
    pub bullet_free: bool,
    pub prose_only: bool,
    pub minimum_word_count_met: bool,
    pub citation_target_met: bool,
    pub actual_word_count: usize,
    pub minimum_word_count: usize,
    pub citation_mentions: usize,
    pub citation_target: usize,
    pub missing_headings: Vec<String>,
    pub banned_phrase_hits: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResearchArtifacts {
    pub markdown_path: Option<String>,
    pub json_path: Option<String>,
    pub pdf_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResearchReport {
    pub topic: String,
    pub tier: String,
    pub deliverable: String,
    pub reference_style: String,
    pub title: String,
    pub citations_target: usize,
    pub total_word_count: usize,
    pub estimated_page_count: usize,
    pub abstract_preview: Option<String>,
    pub plan: ResearchPlan,
    pub sections: Vec<ResearchSection>,
    pub sources: Vec<ResearchSource>,
    pub references: String,
    pub markdown: String,
    pub quality: ResearchQualityReport,
    pub artifacts: Option<ResearchArtifacts>,
}

/// Simulation request body for local testing.
#[derive(Debug, Deserialize)]
pub struct SimulateRequest {
    pub topic: String,
    #[serde(default = "default_tier")]
    pub tier: String,
    pub reference_style: Option<String>,
}

fn default_tier() -> String {
    "complete".to_string()
}

// ── WhatsApp Bridge Models ────────────────────────────────────────

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct WhatsAppBridgeMessage {
    #[serde(rename = "from")]
    pub sender: String,
    #[serde(default)]
    pub body: String,
    #[serde(default)]
    pub push_name: String,
    #[serde(default)]
    pub message_id: String,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct WhatsAppBridgeAudio {
    #[serde(rename = "from")]
    pub sender: String,
    #[serde(default)]
    pub push_name: String,
    #[serde(default)]
    pub audio_base64: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WhatsAppBridgeResponse {
    pub reply: Option<String>,
    pub document_base64: Option<String>,
    pub document_filename: Option<String>,
    pub document_caption: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WhatsAppResearchBrief {
    pub topic: String,
    pub answer: String,
    pub sources: Vec<ResearchSource>,
}

// ── Bot Text Constants ────────────────────────────────────────────

pub const GREETING_MESSAGE: &str = "\
🎓 *Welcome to Professor AI — Your Academic Research Assistant*

I write full, citation-backed academic papers on any topic.
Here's what I can produce:

📋 *Preview (Free)*      → Abstract + Table of Contents
📝 *Starter (₦1,500)*    → + Introduction + Literature Review
📊 *Standard (₦5,000)*   → + Methodology + Findings
📚 *Complete (₦10,000)*  → Full paper with References + PDF

🧪 *BETA TESTING PHASE*
For the next 2 weeks, ALL tiers are *FREE*.
Your feedback helps me improve.

Simply send me your research topic to begin.

Type /help for all commands
Type /terms to read our Terms of Use";

pub const HELP_MESSAGE: &str = "\
📖 *Professor AI — Commands*

/start — Show welcome message
/help — Show this help text
/terms — Terms of Use
/status — Check your current research status
/feedback — Rate your last report

*How to use:*
Just send me a research topic and I'll get to work!

Optional: add a new line like
`Reference style: MLA` or `Reference style: Chicago`";

pub const TERMS_MESSAGE: &str = "\
📜 *Terms of Use — Professor AI*

*1. Academic Integrity*
Reports are research aids, not submissions. You are responsible for proper use per your institution's policies.

*2. No Guarantees*
AI-generated content may contain errors. Always verify citations and facts.

*3. Privacy*
We store your chat ID and topics for service delivery only. We do not share data with third parties.

*4. Fair Use*
During beta testing, abuse (spam, automated requests, reselling) will result in a block.

*5. Pricing*
Current beta pricing is FREE. Paid tiers activate after the testing period. Prices may change.

By using this bot, you agree to these terms.";

pub const RESEARCH_IN_PROGRESS_MESSAGE: &str = "\
⏳ Your previous research is still in progress. Please wait for it to complete before submitting a new topic.";

pub const RATE_LIMIT_MESSAGE: &str = "\
🚫 You've reached the limit of 3 research requests per hour during beta testing. Please try again later.";
