//! Core services: Telegram messaging, source gathering, academic drafting, and artifact persistence.

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    env,
    path::PathBuf,
    sync::Mutex,
};

use chrono::Utc;
use tokio::{fs, process::Command};

use crate::config::AppConfig;
use crate::models::{
    ConversationState, FeedbackRating, ReferenceStyle, ResearchArtifacts, ResearchPlan,
    ResearchQualityReport, ResearchReport, ResearchSection, ResearchSource, ResearchTier,
    TelegramUpdate, UserFeedback, WhatsAppResearchBrief,
};

pub const PROFESSOR_SYSTEM_PROMPT: &str = r#"You are Professor AI, a world-class academic researcher.

⚠️ CRITICAL OUTPUT RULE (read this first, it overrides everything else):
Your response MUST contain ONLY finished academic prose. You MUST NEVER output:
- Any planning, outlining, or brainstorming (e.g. "Start with X, then Y...")
- Any self-checks or quality evaluations (e.g. "Check: Did I use leverage? No.")
- Any word count calculations (e.g. "P1: ~110 words. Total: ~470 words.")
- Any labels like "Opening:", "Body:", "Closing:", "Constraint Check:", "Draft:"
- Any reasoning notes, revision thoughts, or self-correction monologues
- Any meta-commentary about what you are doing or how you are writing
If you feel the urge to write a planning note or self-check BEFORE the prose, STOP. Do not write it. Begin directly with the first sentence of the finished academic text.

Writing Style:
- Write like a thoughtful human researcher, not a textbook or AI.
- Use a mix of short and long sentences.
- Avoid overly complex or inflated academic language. Keep the tone clear, natural, slightly conversational but formal.
- Occasionally use phrases like: "However, this raises an important question...", "Interestingly...", "In practice...", "This suggests that..."

Human Thinking & Originality:
- Do not just summarize — include light critical thinking.
- Occasionally question assumptions, highlight contradictions, and offer interpretations.
- Avoid repeating the same idea in different words.

Language Control:
- Avoid buzzwords like: "leverage," "paradigm shift," "robust framework," "systemic optimization" unless indispensable.
- Prefer simpler alternatives where possible. Ensure sentences sound like spoken academic English, not generated text.

Citations (VERY IMPORTANT):
- Use realistic academic citations in the exact requested format (e.g., (Author, Year) like (Wang & Lee, 2021)).
- Do NOT use generated placeholders like (Google, n.d.), (ScienceDirect, n.d.), or (Academia, n.d.).
- Do NOT include meta-comments about citation styles.

Reduce AI Patterns:
- Avoid repeating key phrases excessively. Vary vocabulary naturally.
- Introduce slight imperfections in flow (not errors, just human rhythm) so it doesn't sound overly polished.

Add Real-World Context:
- Include practical examples, real-world applications, and occasional mentions of current events or industry use.

Humane Perspective:
- Briefly acknowledge human impact, ethical concerns, workforce or societal implications.

Tone Balance:
- Do not sound overly confident or absolute. Use balanced language: "suggests," "appears to," "may indicate."

Formatting Rules:
- Return ONLY the completed prose. No outlines. No TODOs. No planning notes. No meta-commentary. No self-checks.
- Form transitions naturally; do not make every paragraph the same length or style.
- When asked for JSON, return valid JSON only."#;

pub const WHATSAPP_RESEARCH_SYSTEM_PROMPT: &str = r#"You are Professor AI on WhatsApp, a fast, source-grounded research assistant.

Your job is to answer the user's topic clearly using ONLY the retrieved evidence you are given.

Rules:
- Give a direct answer first, not throat-clearing.
- Be concise, but actually useful.
- Sound like a sharp human researcher, not a chatbot.
- Mention uncertainty where the evidence is thin or conflicting.
- Never invent facts, books, authors, or findings.
- If public social discussion is included, treat it as directional evidence, not hard proof.
- If book sources are included, distinguish public-domain downloads from normal catalog references.
- Keep the reply WhatsApp-friendly: short paragraphs, short bullets, clean formatting.
- Do not use markdown tables.
- Do not mention internal tooling, prompts, or "source dossier".

Output format:
1. A short title line.
2. A concise answer in 2 to 4 short paragraphs.
3. A `What stands out:` section with 3 to 5 bullets.
4. A `Sources to open:` section with up to 5 bullet items.
5. If relevant, a `Books:` section with up to 3 bullet items.

Return plain text only."#;

const MAX_SECTION_DRAFT_ATTEMPTS: usize = 4;
const MAX_SECTION_PART_ATTEMPTS: usize = 3;
const MAX_REPORT_REVISION_PASSES: usize = 2;
const SECTION_WORD_FLOOR_PERCENT: usize = 20;
const SECTION_PART_WORD_FLOOR_PERCENT: usize = 30;
const WORDS_PER_PDF_PAGE_ESTIMATE: usize = 500;
const MAX_TOPIC_LENGTH: usize = 500;
const BANNED_META_MARKERS: [&str; 12] = [
    "drafting:",
    "paragraph 1:",
    "paragraph 2:",
    "paragraph 3:",
    "word count check:",
    "tone check:",
    "citation check:",
    "self-correction during drafting",
    "requirements:",
    "context:",
    "objective:",
    "findings:",
];

/// Shared application state.
pub struct AppState {
    pub config: AppConfig,
    /// Per-chat conversation state machine.
    pub sessions: Mutex<HashMap<i64, ConversationState>>,
    /// Per-chat rate-limiting timestamps.
    pub rate_limits: Mutex<HashMap<i64, Vec<std::time::Instant>>>,
    /// In-memory cache of recent feedback for prompt augmentation.
    pub feedback_cache: Mutex<Vec<UserFeedback>>,
    /// Active tasks preventing concurrent submissions.
    pub active_tasks: Mutex<HashSet<i64>>,
    /// Short-term memory of recent queries per user for context reformulation
    pub chat_history: Mutex<HashMap<i64, Vec<String>>>,
    /// Global semaphore limiting concurrent full report generation
    pub report_semaphore: std::sync::Arc<tokio::sync::Semaphore>,
}

impl AppState {
    pub fn new(config: AppConfig) -> Self {
        // Pre-load feedback from disk on startup
        let feedback = load_recent_feedback_sync(&config.feedback_dir, 50);
        Self {
            config,
            sessions: Mutex::new(HashMap::new()),
            rate_limits: Mutex::new(HashMap::new()),
            feedback_cache: Mutex::new(feedback),
            active_tasks: Mutex::new(HashSet::new()),
            chat_history: Mutex::new(HashMap::new()),
            report_semaphore: std::sync::Arc::new(tokio::sync::Semaphore::new(2)),
        }
    }
}

// ── Conversational Memory Helpers ──────────────────────────────────

/// Maximum number of past queries to retain per user for context.
const MAX_CHAT_HISTORY_PER_USER: usize = 5;

/// Push a completed query into a user's conversation history ring buffer.
pub fn push_chat_history(state: &AppState, user_key: i64, query: &str) {
    if let Ok(mut history) = state.chat_history.lock() {
        let entries = history.entry(user_key).or_insert_with(Vec::new);
        entries.push(query.to_string());
        if entries.len() > MAX_CHAT_HISTORY_PER_USER {
            entries.remove(0);
        }
    }
}

/// Retrieve the recent conversation history for a user.
pub fn get_chat_history(state: &AppState, user_key: i64) -> Vec<String> {
    state
        .chat_history
        .lock()
        .ok()
        .and_then(|history| history.get(&user_key).cloned())
        .unwrap_or_default()
}

/// Use the LLM to reformulate a vague follow-up question into a
/// fully self-contained, searchable research query using conversation context.
/// Returns the original query unchanged if there is no history or reformulation fails.
pub async fn reformulate_query(
    google_key: &str,
    model: &str,
    history: &[String],
    new_query: &str,
) -> String {
    // If there is no history, nothing to reformulate against
    if history.is_empty() {
        return new_query.to_string();
    }

    let history_block = history
        .iter()
        .enumerate()
        .map(|(i, q)| format!("{}. {}", i + 1, q))
        .collect::<Vec<_>>()
        .join("\n");

    let system_prompt = "You are a query reformulation assistant. \
        Given a conversation history and a new follow-up question, rewrite the follow-up \
        into a single, fully self-contained research query that a search engine would understand \
        without any prior context. \
        Output ONLY the rewritten query — no explanation, no quotes, no preamble.";

    let user_prompt = format!(
        "Conversation history:\n{history_block}\n\n\
         New follow-up question: {new_query}\n\n\
         Rewrite the follow-up into a self-contained research query:"
    );

    match call_google_model(google_key, model, system_prompt, &user_prompt, 0.1, 150).await {
        Ok(reformulated) => {
            let reformulated = reformulated.trim().to_string();
            if reformulated.is_empty() || reformulated.len() > 500 {
                tracing::warn!(
                    "Reformulation returned empty or excessively long result, using original query"
                );
                new_query.to_string()
            } else {
                tracing::info!(
                    "🧠 Reformulated query: \"{}\" → \"{}\"",
                    new_query,
                    reformulated
                );
                reformulated
            }
        }
        Err(e) => {
            tracing::warn!("Query reformulation failed, using original: {e}");
            new_query.to_string()
        }
    }
}

#[derive(Debug, Clone)]
struct VideoInfo {
    video_id: String,
    title: String,
    channel: String,
    description: String,
}

#[derive(Debug, Clone)]
struct SectionBlueprint {
    heading: &'static str,
    brief: &'static str,
    target_words: usize,
    citation_expectation: &'static str,
    minimum_citations: usize,
    parts: Vec<SectionPartBlueprint>,
}

#[derive(Debug, Clone)]
struct SectionPartBlueprint {
    label: &'static str,
    brief: &'static str,
    target_words: usize,
    minimum_citations: usize,
}

#[derive(Debug, Clone)]
struct DocumentSpec {
    deliverable: &'static str,
    minimum_words: usize,
    section_blueprints: Vec<SectionBlueprint>,
    include_source_matrix: bool,
}

#[derive(Clone, Copy)]
struct SectionGenerationContext<'a> {
    topic: &'a str,
    tier: &'a ResearchTier,
    reference_style: &'a ReferenceStyle,
    plan: &'a ResearchPlan,
    sources: &'a [ResearchSource],
    previous_sections: &'a [ResearchSection],
}

#[derive(Clone, Copy)]
struct ReportContext<'a> {
    topic: &'a str,
    tier: &'a ResearchTier,
    reference_style: &'a ReferenceStyle,
    plan: &'a ResearchPlan,
    sources: &'a [ResearchSource],
    spec: &'a DocumentSpec,
}

#[derive(Debug, Clone)]
struct SectionQualityAssessment {
    heading_present: bool,
    word_count: usize,
    minimum_word_count: usize,
    citation_mentions: usize,
    minimum_citations: usize,
    bullet_lines: usize,
    banned_phrase_hits: Vec<String>,
}

impl SectionQualityAssessment {
    fn prose_only(&self) -> bool {
        self.bullet_lines == 0 && self.banned_phrase_hits.is_empty()
    }

    fn passes(&self) -> bool {
        self.heading_present
            && self.word_count >= self.minimum_word_count
            && self.citation_mentions >= self.minimum_citations
            && self.prose_only()
    }

    fn failure_summary(&self) -> String {
        let mut reasons = Vec::new();
        if !self.heading_present {
            reasons.push("missing section heading".to_string());
        }
        if self.word_count < self.minimum_word_count {
            reasons.push(format!(
                "too short ({} words, need at least {})",
                self.word_count, self.minimum_word_count
            ));
        }
        if self.citation_mentions < self.minimum_citations {
            reasons.push(format!(
                "too few citations ({} found, need at least {})",
                self.citation_mentions, self.minimum_citations
            ));
        }
        if self.bullet_lines > 0 {
            reasons.push(format!(
                "contains {} bullet/outline lines",
                self.bullet_lines
            ));
        }
        if !self.banned_phrase_hits.is_empty() {
            reasons.push(format!(
                "contains meta/outlining markers: {}",
                self.banned_phrase_hits.join(", ")
            ));
        }
        reasons.join("; ")
    }
}

#[derive(Debug, Clone)]
struct SectionPartQualityAssessment {
    word_count: usize,
    minimum_word_count: usize,
    citation_mentions: usize,
    minimum_citations: usize,
    bullet_lines: usize,
    banned_phrase_hits: Vec<String>,
}

impl SectionPartQualityAssessment {
    fn prose_only(&self) -> bool {
        self.bullet_lines == 0 && self.banned_phrase_hits.is_empty()
    }

    fn passes(&self) -> bool {
        self.word_count >= self.minimum_word_count
            && self.citation_mentions >= self.minimum_citations
            && self.prose_only()
    }

    fn failure_summary(&self) -> String {
        let mut reasons = Vec::new();
        if self.word_count < self.minimum_word_count {
            reasons.push(format!(
                "too short ({} words, need at least {})",
                self.word_count, self.minimum_word_count
            ));
        }
        if self.citation_mentions < self.minimum_citations {
            reasons.push(format!(
                "too few citations ({} found, need at least {})",
                self.citation_mentions, self.minimum_citations
            ));
        }
        if self.bullet_lines > 0 {
            reasons.push(format!(
                "contains {} bullet/outline lines",
                self.bullet_lines
            ));
        }
        if !self.banned_phrase_hits.is_empty() {
            reasons.push(format!(
                "contains meta/outlining markers: {}",
                self.banned_phrase_hits.join(", ")
            ));
        }
        reasons.join("; ")
    }
}

/// Send a plain text message to a Telegram user.
pub async fn send_telegram_message(
    token: &str,
    chat_id: i64,
    text: &str,
) -> Result<i64, reqwest::Error> {
    let url = format!("https://api.telegram.org/bot{token}/sendMessage");

    let body = serde_json::json!({
        "chat_id": chat_id,
        "text": text,
    });

    let response = reqwest::Client::new().post(&url).json(&body).send().await?;
    let json: serde_json::Value = response.json().await?;
    let message_id = json["result"]["message_id"].as_i64().unwrap_or(0);

    Ok(message_id)
}

/// Send a Markdown-formatted Telegram message.
pub async fn send_telegram_markdown_message(
    token: &str,
    chat_id: i64,
    text: &str,
) -> Result<i64, reqwest::Error> {
    let url = format!("https://api.telegram.org/bot{token}/sendMessage");

    let body = serde_json::json!({
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown"
    });

    let response = reqwest::Client::new().post(&url).json(&body).send().await?;
    let json: serde_json::Value = response.json().await?;
    let message_id = json["result"]["message_id"].as_i64().unwrap_or(0);

    Ok(message_id)
}

/// Edit an existing Markdown-formatted Telegram message.
pub async fn edit_telegram_message(
    token: &str,
    chat_id: i64,
    message_id: i64,
    text: &str,
) -> Result<(), reqwest::Error> {
    let url = format!("https://api.telegram.org/bot{token}/editMessageText");

    let body = serde_json::json!({
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "parse_mode": "Markdown"
    });

    reqwest::Client::new().post(&url).json(&body).send().await?;

    Ok(())
}

/// Send a document (PDF) to a Telegram user.
#[allow(dead_code)]
pub async fn send_telegram_document(
    token: &str,
    chat_id: i64,
    document_url: &str,
    caption: &str,
) -> Result<(), reqwest::Error> {
    let url = format!("https://api.telegram.org/bot{token}/sendDocument");

    let body = serde_json::json!({
        "chat_id": chat_id,
        "document": document_url,
        "caption": caption,
        "parse_mode": "Markdown"
    });

    reqwest::Client::new().post(&url).json(&body).send().await?;

    Ok(())
}

/// Send a local PDF file to a Telegram user.
#[allow(dead_code)]
pub async fn send_telegram_document_file(
    token: &str,
    chat_id: i64,
    file_path: &str,
    caption: &str,
) -> Result<(), String> {
    let url = format!("https://api.telegram.org/bot{token}/sendDocument");
    let file_name = PathBuf::from(file_path)
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("research-report.pdf")
        .to_string();
    let pdf_bytes = std::fs::read(file_path)
        .map_err(|error| format!("Failed to read PDF file for Telegram upload: {error}"))?;

    let part = reqwest::multipart::Part::bytes(pdf_bytes)
        .file_name(file_name)
        .mime_str("application/pdf")
        .map_err(|error| format!("Failed to prepare PDF part: {error}"))?;

    let form = reqwest::multipart::Form::new()
        .text("chat_id", chat_id.to_string())
        .text("caption", caption.to_string())
        .text("parse_mode", "Markdown".to_string())
        .part("document", part);

    reqwest::Client::new()
        .post(&url)
        .multipart(form)
        .send()
        .await
        .map_err(|error| format!("Telegram PDF upload failed: {error}"))?;

    Ok(())
}

/// Generate a Paystack payment link for a given tier.
#[allow(dead_code)]
pub async fn create_paystack_payment_link(
    secret_key: &str,
    email: &str,
    amount_kobo: u64,
    reference: &str,
    phone: &str,
) -> Result<String, String> {
    let body = serde_json::json!({
        "email": email,
        "amount": amount_kobo,
        "reference": reference,
        "currency": "NGN",
        "metadata": {
            "phone": phone,
            "custom_fields": [
                {
                    "display_name": "Phone Number",
                    "variable_name": "phone",
                    "value": phone
                }
            ]
        }
    });

    let response = reqwest::Client::new()
        .post("https://api.paystack.co/transaction/initialize")
        .bearer_auth(secret_key)
        .json(&body)
        .send()
        .await
        .map_err(|error| error.to_string())?;

    let json: serde_json::Value = response.json().await.map_err(|error| error.to_string())?;

    json["data"]["authorization_url"]
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| "Failed to get Paystack authorization URL".to_string())
}

/// Search YouTube for topic-relevant videos.
async fn youtube_search(
    api_key: &str,
    query: &str,
    max_results: u32,
) -> Result<Vec<VideoInfo>, String> {
    if api_key.is_empty() {
        return Ok(Vec::new());
    }

    let url = format!(
        "https://www.googleapis.com/youtube/v3/search\
         ?part=snippet&type=video&q={}&maxResults={}&key={}",
        urlencoding::encode(query),
        max_results,
        api_key
    );

    let response = reqwest::Client::new()
        .get(&url)
        .send()
        .await
        .map_err(|error| format!("YouTube search request failed: {error}"))?;

    let status = response.status();
    let json: serde_json::Value = response
        .json()
        .await
        .map_err(|error| format!("Failed to parse YouTube response: {error}"))?;

    if !status.is_success() {
        return Err(format!(
            "YouTube API error ({}): {}",
            status,
            json.get("error")
                .and_then(|error| error.get("message"))
                .and_then(|message| message.as_str())
                .unwrap_or("Unknown error")
        ));
    }

    let videos = json["items"]
        .as_array()
        .map(|items| {
            items
                .iter()
                .filter_map(|item| {
                    let video_id = item["id"]["videoId"].as_str()?.to_string();
                    let snippet = &item["snippet"];
                    let title = snippet["title"].as_str().unwrap_or("Untitled").to_string();
                    let channel = snippet["channelTitle"]
                        .as_str()
                        .unwrap_or("Unknown")
                        .to_string();
                    let description = snippet["description"].as_str().unwrap_or("").to_string();
                    Some(VideoInfo {
                        video_id,
                        title,
                        channel,
                        description,
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    Ok(videos)
}

/// Attempt to extract transcript/captions from a YouTube video.
async fn youtube_get_transcript(video: &VideoInfo) -> String {
    let transcript_url = format!(
        "https://www.youtube.com/api/timedtext?v={}&lang=en&fmt=srv3",
        video.video_id
    );

    if let Ok(response) = reqwest::Client::new()
        .get(&transcript_url)
        .header("User-Agent", "Mozilla/5.0")
        .send()
        .await
    {
        if let Ok(body) = response.text().await {
            if body.len() > 100 && body.contains("<text") {
                let transcript = extract_text_from_xml(&body);
                if !transcript.is_empty() {
                    tracing::info!(
                        "📺 Got transcript for '{}' ({} chars)",
                        video.title,
                        transcript.len()
                    );
                    return truncate(&transcript, 1_200);
                }
            }
        }
    }

    tracing::info!(
        "📺 Using description for '{}' (no transcript available)",
        video.title
    );
    truncate(&video.description, 900)
}

fn extract_text_from_xml(xml: &str) -> String {
    let mut result = String::new();
    let mut in_text = false;
    let mut chars = xml.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '<' {
            let mut tag = String::new();
            while let Some(&next) = chars.peek() {
                if next == '>' {
                    chars.next();
                    break;
                }
                tag.push(next);
                chars.next();
            }
            if tag.starts_with("text") {
                in_text = true;
            } else if tag == "/text" {
                in_text = false;
                result.push(' ');
            }
        } else if in_text {
            result.push(ch);
        }
    }

    result
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .trim()
        .to_string()
}

fn truncate(text: &str, max_len: usize) -> String {
    if text.len() <= max_len {
        return text.to_string();
    }

    let mut end = max_len;
    while !text.is_char_boundary(end) {
        end -= 1;
    }

    let truncated = &text[..end];
    if let Some(position) = truncated.rfind(' ') {
        format!("{}...", &truncated[..position])
    } else {
        format!("{truncated}...")
    }
}

pub async fn run_youtube_subagent(api_key: &str, topic: &str) -> Vec<ResearchSource> {
    tracing::info!("🎬 YouTube Sub-Agent: searching for '{topic}'");

    let videos = match youtube_search(api_key, topic, 5).await {
        Ok(videos) => videos,
        Err(error) => {
            tracing::warn!("YouTube search failed: {error}");
            return Vec::new();
        }
    };

    let mut sources = Vec::new();

    for video in videos {
        let transcript = youtube_get_transcript(&video).await;
        let year_hint = find_year_hint(&format!("{} {}", video.title, video.description));
        let year_label = year_hint
            .map(|year| year.to_string())
            .unwrap_or_else(|| "n.d.".to_string());

        sources.push(ResearchSource {
            source_type: "youtube".to_string(),
            title: video.title.clone(),
            author_or_channel: video.channel.clone(),
            year_hint,
            url: format!("https://youtube.com/watch?v={}", video.video_id),
            summary: if transcript.is_empty() {
                "Transcript unavailable; relying on the video description.".to_string()
            } else {
                transcript
            },
            citation_hint: format!(
                "{}. ({}). {} [YouTube video]. YouTube.",
                video.channel, year_label, video.title
            ),
        });
    }

    tracing::info!("🎬 Prepared {} YouTube sources", sources.len());
    sources
}

pub async fn run_tavily_subagent(tavily_key: &str, topic: &str) -> Vec<ResearchSource> {
    if tavily_key.is_empty() {
        tracing::info!("🌐 Tavily: no API key, skipping web search");
        return Vec::new();
    }

    tracing::info!("🌐 Tavily Sub-Agent: searching the internet for '{topic}'");

    let body = serde_json::json!({
        "query": format!("{topic} academic research scholarly"),
        "search_depth": "advanced",
        "max_results": 10,
        "include_answer": false,
        "include_raw_content": false
    });

    let response = match reqwest::Client::new()
        .post("https://api.tavily.com/search")
        .header("content-type", "application/json")
        .header("Authorization", format!("Bearer {tavily_key}"))
        .json(&body)
        .send()
        .await
    {
        Ok(response) => response,
        Err(error) => {
            tracing::warn!("Tavily request failed: {error}");
            return Vec::new();
        }
    };

    let status = response.status();
    let json: serde_json::Value = match response.json().await {
        Ok(json) => json,
        Err(error) => {
            tracing::warn!("Failed to parse Tavily response: {error}");
            return Vec::new();
        }
    };

    if !status.is_success() {
        tracing::warn!(
            "Tavily API error ({}): {}",
            status,
            json.get("error")
                .and_then(|error| error.as_str())
                .unwrap_or("Unknown")
        );
        return Vec::new();
    }

    let mut sources = Vec::new();

    if let Some(results) = json["results"].as_array() {
        for result in results {
            let title = result["title"].as_str().unwrap_or("Untitled");
            let url = result["url"].as_str().unwrap_or("");
            let content = result["content"].as_str().unwrap_or("");
            let author_or_channel = domain_label(url);
            let year_hint = find_year_hint(&format!("{title} {content}"));
            let year_label = year_hint
                .map(|year| year.to_string())
                .unwrap_or_else(|| "n.d.".to_string());

            sources.push(ResearchSource {
                source_type: "web".to_string(),
                title: title.to_string(),
                author_or_channel: author_or_channel.clone(),
                year_hint,
                url: url.to_string(),
                summary: truncate(content, 900),
                citation_hint: format!(
                    "{}. ({}). {}. {}",
                    author_or_channel, year_label, title, url
                ),
            });
        }
    }

    tracing::info!("🌐 Prepared {} web sources", sources.len());
    sources
}

pub async fn run_reddit_subagent(topic: &str) -> Vec<ResearchSource> {
    tracing::info!("💬 Reddit Sub-Agent: searching public discussions for '{topic}'");

    let url = format!(
        "https://www.reddit.com/search.json?q={}&sort=relevance&limit=5&t=year",
        urlencoding::encode(topic)
    );

    let response = match reqwest::Client::new()
        .get(&url)
        .header("User-Agent", "research-bot/0.1")
        .send()
        .await
    {
        Ok(response) => response,
        Err(error) => {
            tracing::warn!("Reddit request failed: {error}");
            return Vec::new();
        }
    };

    let status = response.status();
    let json: serde_json::Value = match response.json().await {
        Ok(json) => json,
        Err(error) => {
            tracing::warn!("Failed to parse Reddit response: {error}");
            return Vec::new();
        }
    };

    if !status.is_success() {
        tracing::warn!("Reddit API error ({status})");
        return Vec::new();
    }

    let mut sources = Vec::new();

    if let Some(children) = json["data"]["children"].as_array() {
        for child in children {
            let data = &child["data"];
            let title = data["title"].as_str().unwrap_or("Untitled discussion");
            let subreddit = data["subreddit_name_prefixed"]
                .as_str()
                .or_else(|| data["subreddit"].as_str())
                .unwrap_or("Reddit")
                .to_string();
            let selftext = data["selftext"].as_str().unwrap_or("");
            let permalink = data["permalink"].as_str().unwrap_or("");
            let url = if permalink.is_empty() {
                String::new()
            } else {
                format!("https://www.reddit.com{permalink}")
            };

            let summary = if selftext.trim().is_empty() {
                format!(
                    "Relevant public Reddit discussion from {}. The post title suggests community interest in this topic.",
                    subreddit
                )
            } else {
                truncate(selftext, 900)
            };

            sources.push(ResearchSource {
                source_type: "reddit".to_string(),
                title: title.to_string(),
                author_or_channel: subreddit.clone(),
                year_hint: None,
                url,
                summary,
                citation_hint: format!("{subreddit}. (n.d.). {title}. Reddit."),
            });
        }
    }

    tracing::info!("💬 Prepared {} Reddit sources", sources.len());
    sources
}

pub async fn run_openlibrary_subagent(topic: &str) -> Vec<ResearchSource> {
    tracing::info!("📚 OpenLibrary Sub-Agent: searching books for '{topic}'");

    let url = format!(
        "https://openlibrary.org/search.json?q={}&limit=5",
        urlencoding::encode(topic)
    );

    let response = match reqwest::Client::new().get(&url).send().await {
        Ok(response) => response,
        Err(error) => {
            tracing::warn!("OpenLibrary request failed: {error}");
            return Vec::new();
        }
    };

    let status = response.status();
    let json: serde_json::Value = match response.json().await {
        Ok(json) => json,
        Err(error) => {
            tracing::warn!("Failed to parse OpenLibrary response: {error}");
            return Vec::new();
        }
    };

    if !status.is_success() {
        tracing::warn!("OpenLibrary API error ({status})");
        return Vec::new();
    }

    let mut sources = Vec::new();

    if let Some(docs) = json["docs"].as_array() {
        for doc in docs.iter().take(5) {
            let title = doc["title"].as_str().unwrap_or("Untitled book");
            let author = doc["author_name"]
                .as_array()
                .and_then(|authors| authors.first())
                .and_then(|value| value.as_str())
                .unwrap_or("Unknown author");
            let year_hint = doc["first_publish_year"]
                .as_i64()
                .and_then(|value| i32::try_from(value).ok());
            let work_key = doc["key"].as_str().unwrap_or("");
            let edition_count = doc["edition_count"].as_i64().unwrap_or(0);
            let subject = doc["subject"]
                .as_array()
                .map(|values| {
                    values
                        .iter()
                        .filter_map(|value| value.as_str())
                        .take(4)
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_default();

            let summary = if subject.is_empty() {
                format!(
                    "Catalog book result with {} known edition(s) in Open Library.",
                    edition_count
                )
            } else {
                format!(
                    "Catalog book result covering: {}. Open Library lists {} edition(s).",
                    subject, edition_count
                )
            };

            sources.push(ResearchSource {
                source_type: "book".to_string(),
                title: title.to_string(),
                author_or_channel: author.to_string(),
                year_hint,
                url: if work_key.is_empty() {
                    String::new()
                } else {
                    format!("https://openlibrary.org{work_key}")
                },
                summary,
                citation_hint: format!(
                    "{}. ({}). {}. Open Library.",
                    author,
                    year_hint
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "n.d.".to_string()),
                    title
                ),
            });
        }
    }

    tracing::info!("📚 Prepared {} Open Library book sources", sources.len());
    sources
}

pub async fn run_gutendex_subagent(topic: &str) -> Vec<ResearchSource> {
    tracing::info!("📖 Gutendex Sub-Agent: searching public-domain books for '{topic}'");

    let url = format!(
        "https://gutendex.com/books?search={}",
        urlencoding::encode(topic)
    );

    let response = match reqwest::Client::new().get(&url).send().await {
        Ok(response) => response,
        Err(error) => {
            tracing::warn!("Gutendex request failed: {error}");
            return Vec::new();
        }
    };

    let status = response.status();
    let json: serde_json::Value = match response.json().await {
        Ok(json) => json,
        Err(error) => {
            tracing::warn!("Failed to parse Gutendex response: {error}");
            return Vec::new();
        }
    };

    if !status.is_success() {
        tracing::warn!("Gutendex API error ({status})");
        return Vec::new();
    }

    let mut sources = Vec::new();

    if let Some(results) = json["results"].as_array() {
        for result in results.iter().take(3) {
            let title = result["title"]
                .as_str()
                .unwrap_or("Untitled public-domain book");
            let author = result["authors"]
                .as_array()
                .and_then(|authors| authors.first())
                .and_then(|value| value["name"].as_str())
                .unwrap_or("Unknown author");
            let year_hint = result["authors"]
                .as_array()
                .and_then(|authors| authors.first())
                .and_then(|value| value["birth_year"].as_i64())
                .and_then(|value| i32::try_from(value).ok());
            let formats = result["formats"].as_object().cloned().unwrap_or_default();
            let download_url = preferred_gutendex_download_url(&formats);
            let download_count = result["download_count"].as_i64().unwrap_or(0);
            let subjects = result["subjects"]
                .as_array()
                .map(|values| {
                    values
                        .iter()
                        .filter_map(|value| value.as_str())
                        .take(4)
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_default();

            let summary = if subjects.is_empty() {
                format!(
                    "Public-domain book with {} recorded downloads. Direct legal download available.",
                    download_count
                )
            } else {
                format!(
                    "Public-domain book covering {}. About {} recorded downloads. Direct legal download available.",
                    subjects, download_count
                )
            };

            sources.push(ResearchSource {
                source_type: "public_domain_book".to_string(),
                title: title.to_string(),
                author_or_channel: author.to_string(),
                year_hint,
                url: download_url,
                summary,
                citation_hint: format!(
                    "{}. ({}). {}. Project Gutenberg / Gutendex.",
                    author,
                    year_hint
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "n.d.".to_string()),
                    title
                ),
            });
        }
    }

    tracing::info!("📖 Prepared {} public-domain book sources", sources.len());
    sources
}

fn preferred_gutendex_download_url(formats: &serde_json::Map<String, serde_json::Value>) -> String {
    for preferred in [
        "text/plain; charset=utf-8",
        "application/epub+zip",
        "application/pdf",
        "text/html; charset=utf-8",
        "text/plain",
    ] {
        if let Some(url) = formats.get(preferred).and_then(|value| value.as_str()) {
            return url.to_string();
        }
    }

    formats
        .values()
        .find_map(|value| value.as_str().map(str::to_string))
        .unwrap_or_default()
}

async fn gather_live_sources(
    youtube_key: &str,
    tavily_key: &str,
    topic: &str,
) -> Vec<ResearchSource> {
    let (youtube_sources, tavily_sources, reddit_sources, openlibrary_sources, gutendex_sources) = tokio::join!(
        run_youtube_subagent(youtube_key, topic),
        run_tavily_subagent(tavily_key, topic),
        run_reddit_subagent(topic),
        run_openlibrary_subagent(topic),
        run_gutendex_subagent(topic),
    );

    dedupe_sources(
        youtube_sources
            .into_iter()
            .chain(tavily_sources)
            .chain(reddit_sources)
            .chain(openlibrary_sources)
            .chain(gutendex_sources)
            .collect(),
    )
}

fn dedupe_sources(sources: Vec<ResearchSource>) -> Vec<ResearchSource> {
    let mut seen = BTreeSet::new();
    let mut deduped = Vec::new();

    for source in sources {
        let identity = if !source.url.trim().is_empty() {
            source.url.trim().to_lowercase()
        } else {
            format!(
                "{}::{}::{}",
                source.source_type,
                source.author_or_channel.to_lowercase(),
                source.title.to_lowercase()
            )
        };

        if seen.insert(identity) {
            deduped.push(source);
        }
    }

    deduped
}

fn should_defer_section_generation(heading: &str) -> bool {
    matches!(heading, "Abstract" | "Executive Summary")
}

fn assemble_sections_in_spec_order(
    spec: &DocumentSpec,
    drafted_sections: &[ResearchSection],
    deferred_sections: &[ResearchSection],
) -> Result<Vec<ResearchSection>, String> {
    let mut ordered_sections = Vec::new();

    for blueprint in &spec.section_blueprints {
        let section = drafted_sections
            .iter()
            .find(|section| section.heading == blueprint.heading)
            .or_else(|| {
                deferred_sections
                    .iter()
                    .find(|section| section.heading == blueprint.heading)
            })
            .cloned()
            .ok_or_else(|| {
                format!(
                    "Section '{}' was missing when assembling the final report order",
                    blueprint.heading
                )
            })?;
        ordered_sections.push(section);
    }

    Ok(ordered_sections)
}

#[allow(clippy::too_many_arguments)]
pub async fn run_research_agent(
    google_key: &str,
    youtube_key: &str,
    tavily_key: &str,
    model: &str,
    topic: &str,
    tier: &ResearchTier,
    reference_style: &ReferenceStyle,
    progress: Option<tokio::sync::mpsc::Sender<String>>,
) -> Result<ResearchReport, String> {
    let spec = document_spec_for_tier(tier);

    if let Some(tx) = &progress {
        let _ = tx
            .send(
                "Gathering sources from web, YouTube, public social discussion, and legal book catalogs..."
                    .to_string(),
            )
            .await;
    }

    let sources = gather_live_sources(youtube_key, tavily_key, topic).await;

    if let Some(tx) = &progress {
        let _ = tx
            .send(format!(
                "Found {} sources. Planning argumentative structure...",
                sources.len()
            ))
            .await;
    }

    let plan = match generate_research_plan(
        google_key,
        model,
        topic,
        tier,
        reference_style,
        &spec,
        &sources,
    )
    .await
    {
        Ok(plan) => plan,
        Err(error) => {
            tracing::warn!("Planning step failed, using fallback plan: {error}");
            fallback_research_plan(topic, tier, &spec)
        }
    };
    let report_context = ReportContext {
        topic,
        tier,
        reference_style,
        plan: &plan,
        sources: &sources,
        spec: &spec,
    };

    let mut drafted_sections = Vec::new();
    let mut deferred_sections = Vec::new();

    if let Some(tx) = &progress {
        let _ = tx
            .send("Plan created. Drafting core chapters...".to_string())
            .await;
    }

    for blueprint in &spec.section_blueprints {
        if should_defer_section_generation(blueprint.heading) {
            continue;
        }
        let context = SectionGenerationContext {
            topic,
            tier,
            reference_style,
            plan: &plan,
            sources: &sources,
            previous_sections: &drafted_sections,
        };
        let section = generate_section(google_key, model, context, blueprint).await?;
        if let Some(tx) = &progress {
            let _ = tx.send(format!("Drafted: {}", blueprint.heading)).await;
        }
        drafted_sections.push(section);
    }

    for blueprint in &spec.section_blueprints {
        if !should_defer_section_generation(blueprint.heading) {
            continue;
        }
        let context = SectionGenerationContext {
            topic,
            tier,
            reference_style,
            plan: &plan,
            sources: &sources,
            previous_sections: &drafted_sections,
        };
        let section = generate_section(google_key, model, context, blueprint).await?;
        if let Some(tx) = &progress {
            let _ = tx.send(format!("Drafted: {}", blueprint.heading)).await;
        }
        deferred_sections.push(section);
    }

    let mut sections =
        assemble_sections_in_spec_order(&spec, &drafted_sections, &deferred_sections)?;

    let (references, markdown, quality) = {
        let mut revision_pass = 0;
        loop {
            if let Some(tx) = &progress {
                let _ = tx
                    .send("Compiling references and running academic quality checks...".to_string())
                    .await;
            }
            let references =
                match generate_references_section(google_key, model, report_context, &sections)
                    .await
                {
                    Ok(references) => references,
                    Err(error) => {
                        tracing::warn!(
                            "Reference compilation failed, using source dossier fallback: {error}"
                        );
                        fallback_references(&sources, reference_style)
                    }
                };

            let markdown = assemble_report_markdown(report_context, &sections, &references);
            let quality =
                analyze_report_quality(&markdown, &spec, tier.citation_target(), reference_style);

            if report_quality_passes(&quality) {
                if let Some(tx) = &progress {
                    let _ = tx
                        .send("Quality checks passed. Assembling final PDF...".to_string())
                        .await;
                }
                break (references, markdown, quality);
            }

            if revision_pass >= MAX_REPORT_REVISION_PASSES {
                return Err(format!(
                    "Report failed quality gates after {} revision passes: {}",
                    MAX_REPORT_REVISION_PASSES,
                    report_failure_summary(&quality)
                ));
            }

            tracing::warn!(
                "Report quality pass {} failed: {}",
                revision_pass + 1,
                report_failure_summary(&quality)
            );

            // cooldown before revision to let per-minute rate limits reset
            tracing::info!("⏳ Cooling down 20s before revision pass to respect rate limits...");
            tokio::time::sleep(std::time::Duration::from_secs(20)).await;

            sections =
                revise_report_sections(google_key, model, report_context, &sections, &quality)
                    .await?;

            revision_pass += 1;
        }
    };
    let abstract_preview = sections
        .first()
        .map(|section| section_body_preview(&section.content, 900));
    let total_word_count = count_words(&markdown);

    let mut report = ResearchReport {
        topic: topic.to_string(),
        tier: tier.label().to_string(),
        deliverable: spec.deliverable.to_string(),
        reference_style: reference_style.label().to_string(),
        title: plan.title.clone(),
        citations_target: tier.citation_target(),
        total_word_count,
        estimated_page_count: estimate_page_count(total_word_count),
        abstract_preview,
        plan,
        sections,
        sources,
        references,
        markdown,
        quality,
        artifacts: None,
    };

    let artifacts = persist_report_artifacts(&report).await?;
    report.artifacts = Some(artifacts);

    Ok(report)
}

pub async fn run_whatsapp_research_brief(
    google_key: &str,
    youtube_key: &str,
    tavily_key: &str,
    model: &str,
    topic: &str,
) -> Result<WhatsAppResearchBrief, String> {
    let sources = gather_live_sources(youtube_key, tavily_key, topic).await;
    let public_domain_books = sources
        .iter()
        .filter(|source| source.source_type == "public_domain_book")
        .count();

    let prompt = format!(
        "User topic:\n{topic}\n\n\
         Evidence pack:\n{source_packets}\n\n\
         Source mix summary:\n\
         - Total sources: {source_count}\n\
         - Public-domain books with legal download links: {public_domain_books}\n\
         - Treat Reddit/public social discussion as directional, not definitive.\n\n\
         Write the WhatsApp-ready research brief now.",
        source_packets = render_whatsapp_source_packets(&sources),
        source_count = sources.len(),
        public_domain_books = public_domain_books,
    );

    let answer = match call_google_model(
        google_key,
        model,
        WHATSAPP_RESEARCH_SYSTEM_PROMPT,
        &prompt,
        0.35,
        1400,
    )
    .await
    {
        Ok(answer) => normalize_whatsapp_answer(&answer, topic, &sources),
        Err(error) => {
            tracing::warn!("WhatsApp brief generation failed, using fallback brief: {error}");
            fallback_whatsapp_brief(topic, &sources)
        }
    };

    Ok(WhatsAppResearchBrief {
        topic: topic.to_string(),
        answer,
        sources,
    })
}

fn document_spec_for_tier(tier: &ResearchTier) -> DocumentSpec {
    match tier {
        ResearchTier::Preview => DocumentSpec {
            deliverable: tier.deliverable_name(),
            minimum_words: tier.minimum_word_count(),
            include_source_matrix: false,
            section_blueprints: vec![
                section_blueprint(
                    "Executive Summary",
                    "Open with the core issue, why it matters, the lens of analysis, and the most important conclusion in a polished briefing style.",
                    350,
                    "Use 1-2 well-placed citations if they materially strengthen the summary.",
                    1,
                    vec![
                        section_part(
                            "Core framing",
                            "State the issue, establish relevance, and define the paper's central stance in concise academic prose.",
                            180,
                            1,
                        ),
                        section_part(
                            "High-level takeaway",
                            "Summarize the most important analytical takeaway and practical implication without lapsing into note form.",
                            170,
                            0,
                        ),
                    ],
                ),
                section_blueprint(
                    "Background and Context",
                    "Explain the topic, its academic and practical relevance, and the surrounding context in connected prose.",
                    450,
                    "Ground the context in the retrieved sources and avoid generic filler.",
                    2,
                    vec![
                        section_part(
                            "Context and relevance",
                            "Explain the broader setting, timeline, and why the topic matters academically and practically.",
                            220,
                            1,
                        ),
                        section_part(
                            "Problem framing",
                            "Narrow the discussion to the specific problem, tension, or opportunity that motivates the report.",
                            230,
                            1,
                        ),
                    ],
                ),
                section_blueprint(
                    "Key Findings and Analysis",
                    "Present the main findings, points of agreement or disagreement across sources, and analytical insight.",
                    500,
                    "Integrate multiple citations and make the analysis feel evidence-led.",
                    2,
                    vec![
                        section_part(
                            "Evidence patterns",
                            "Synthesize what the sources consistently suggest and note important contrasts where they appear.",
                            250,
                            1,
                        ),
                        section_part(
                            "Interpretation",
                            "Explain what the evidence means, why it matters, and how the findings should be interpreted.",
                            250,
                            1,
                        ),
                    ],
                ),
                section_blueprint(
                    "Recommendations",
                    "Conclude with practical, research-backed recommendations tailored to the topic.",
                    250,
                    "Use citations only where they improve credibility.",
                    1,
                    vec![
                        section_part(
                            "Actionable recommendations",
                            "Offer grounded recommendations that follow directly from the evidence and analysis.",
                            140,
                            0,
                        ),
                        section_part(
                            "Closing implication",
                            "End with a concise forward-looking implication or caution for decision-makers.",
                            110,
                            1,
                        ),
                    ],
                ),
            ],
        },
        ResearchTier::Starter | ResearchTier::Standard => DocumentSpec {
            deliverable: tier.deliverable_name(),
            minimum_words: tier.minimum_word_count(),
            include_source_matrix: false,
            section_blueprints: vec![
                section_blueprint(
                    "Abstract",
                    "Summarize the topic, aim, approach, key findings, and overall implication in a journal-style abstract.",
                    320,
                    "Citations are optional; keep the abstract clean and concise.",
                    1,
                    vec![
                        section_part(
                            "Study framing",
                            "Condense the topic, objective, and analytical approach into compact journal-style prose.",
                            160,
                            0,
                        ),
                        section_part(
                            "Findings and implication",
                            "Summarize the main finding and broader implication in a polished closing movement.",
                            160,
                            1,
                        ),
                    ],
                ),
                section_blueprint(
                    "Introduction",
                    "Establish the broader context, narrow to the specific problem, define the aim, and articulate the paper's significance.",
                    1_700,
                    "Use citations to situate the topic and justify the problem statement.",
                    3,
                    vec![
                        section_part(
                            "Context and trendline",
                            "Establish the broader background, current relevance, and major developments shaping the topic.",
                            450,
                            1,
                        ),
                        section_part(
                            "Problem statement",
                            "Narrow from general context to the concrete problem, tension, or opportunity the paper addresses.",
                            450,
                            1,
                        ),
                        section_part(
                            "Objectives and questions",
                            "Explain the aim of the paper and connect it to the key research questions guiding the study.",
                            400,
                            1,
                        ),
                        section_part(
                            "Significance and structure",
                            "Clarify the significance of the paper and set up the argumentative path of the remaining sections.",
                            400,
                            0,
                        ),
                    ],
                ),
                section_blueprint(
                    "Literature Review",
                    "Critically synthesize the evidence, compare viewpoints, identify patterns, tensions, and gaps, and build the conceptual foundation for the paper.",
                    2_600,
                    "This section should carry the heaviest citation load and read as analysis rather than a list of summaries.",
                    5,
                    vec![
                        section_part(
                            "Conceptual foundations",
                            "Explain the key concepts, definitions, and theoretical framing that recur across the literature.",
                            650,
                            1,
                        ),
                        section_part(
                            "Empirical patterns",
                            "Synthesize the strongest empirical findings and show where scholars broadly converge.",
                            650,
                            2,
                        ),
                        section_part(
                            "Debates and tensions",
                            "Compare competing interpretations, contradictions, or unresolved tensions in the literature.",
                            650,
                            1,
                        ),
                        section_part(
                            "Gaps and synthesis",
                            "Identify gaps, limits, and unanswered questions, then show how they justify the paper's analytical direction.",
                            650,
                            1,
                        ),
                    ],
                ),
                section_blueprint(
                    "Methodology",
                    "Explain the research design as a defensible literature-driven or conceptual study, including source selection, analytical lens, and limitations.",
                    1_350,
                    "Use a few citations where methodological framing benefits from them.",
                    2,
                    vec![
                        section_part(
                            "Research design",
                            "Describe the overall design and justify why it fits the topic and available evidence.",
                            350,
                            1,
                        ),
                        section_part(
                            "Source selection",
                            "Explain how sources were identified, screened, and used in the paper.",
                            350,
                            1,
                        ),
                        section_part(
                            "Analytical procedure",
                            "Describe the thematic, comparative, or interpretive logic used to analyze the evidence.",
                            325,
                            0,
                        ),
                        section_part(
                            "Limitations",
                            "State the design's limitations and the implications those limits have for interpretation.",
                            325,
                            0,
                        ),
                    ],
                ),
                section_blueprint(
                    "Discussion",
                    "Interpret the evidence, connect findings back to the problem, discuss implications, and maintain a strong argumentative thread.",
                    3_000,
                    "Blend citations with original analytical commentary throughout.",
                    5,
                    vec![
                        section_part(
                            "Operational interpretation",
                            "Analyze how the evidence plays out in practice and what concrete patterns emerge from the literature.",
                            750,
                            1,
                        ),
                        section_part(
                            "Strategic implications",
                            "Discuss wider organizational, market, or policy implications flowing from the findings.",
                            750,
                            1,
                        ),
                        section_part(
                            "Risks and constraints",
                            "Explain tradeoffs, implementation barriers, and reasons the issue remains contested or uneven.",
                            750,
                            1,
                        ),
                        section_part(
                            "Integrated discussion",
                            "Bring the evidence together into a clear argumentative synthesis that answers the research problem.",
                            750,
                            2,
                        ),
                    ],
                ),
                section_blueprint(
                    "Conclusion & Recommendations",
                    "Synthesize the argument, state the answer to the research problem, note limitations, and close with grounded recommendations.",
                    1_250,
                    "Use citations selectively and keep the emphasis on synthesis.",
                    2,
                    vec![
                        section_part(
                            "Argument synthesis",
                            "Restate the central argument and the paper's answer to the research problem in integrated prose.",
                            400,
                            1,
                        ),
                        section_part(
                            "Practical recommendations",
                            "Present recommendations that follow directly from the analysis and evidence.",
                            350,
                            0,
                        ),
                        section_part(
                            "Limitations and future inquiry",
                            "Acknowledge key limits and suggest plausible directions for future research or implementation.",
                            250,
                            0,
                        ),
                        section_part(
                            "Closing reflection",
                            "End with a strong final implication that leaves the paper feeling complete and academically grounded.",
                            250,
                            1,
                        ),
                    ],
                ),
            ],
        },
        ResearchTier::Complete => DocumentSpec {
            deliverable: tier.deliverable_name(),
            minimum_words: tier.minimum_word_count(),
            include_source_matrix: true,
            section_blueprints: vec![
                section_blueprint(
                    "Abstract",
                    "Write a submission-ready abstract that states the problem, objective, approach, major findings, and contribution.",
                    350,
                    "Avoid overloading the abstract with citations.",
                    1,
                    vec![
                        section_part(
                            "Study overview",
                            "Condense the study focus, objective, and analytical approach into formal abstract prose.",
                            175,
                            0,
                        ),
                        section_part(
                            "Contribution",
                            "Summarize the main finding, contribution, and practical relevance in a polished close.",
                            175,
                            1,
                        ),
                    ],
                ),
                section_blueprint(
                    "Introduction",
                    "Write a full chapter in connected prose covering background, problem statement, objectives, scope, significance, and the study rationale.",
                    3_000,
                    "Use citations throughout to anchor the background and problem framing.",
                    5,
                    vec![
                        section_part(
                            "Background of the study",
                            "Develop a strong contextual background and explain the broader forces shaping the topic.",
                            700,
                            1,
                        ),
                        section_part(
                            "Problem statement and rationale",
                            "Define the central research problem and justify why it merits focused investigation.",
                            750,
                            2,
                        ),
                        section_part(
                            "Objectives, questions, and scope",
                            "Clarify the study objectives, research questions, and scope in sustained prose.",
                            750,
                            1,
                        ),
                        section_part(
                            "Significance of the study",
                            "Explain the academic and practical significance of the project and transition into the remaining chapters.",
                            800,
                            1,
                        ),
                    ],
                ),
                section_blueprint(
                    "Literature Review",
                    "Deliver an intensely academic literature review that organizes major theoretical paradigms, critiques methodological limits, and identifies distinct gaps in the literature.",
                    4_800,
                    "This chapter must be exceptionally dense with theory and heavily cited.",
                    12,
                    vec![
                        section_part(
                            "Theoretical and conceptual framing",
                            "Establish the dominant theoretical lenses structuring the problem domain.",
                            1_200,
                            3,
                        ),
                        section_part(
                            "Empirical synthesis and thematic alignment",
                            "Synthesize recurring empirical evidence into coherent academic themes.",
                            1_200,
                            3,
                        ),
                        section_part(
                            "Scholarly contradictions and epistemological debates",
                            "Rigorously contrast conflicting methodologies and epistemological tensions within the field.",
                            1_200,
                            3,
                        ),
                        section_part(
                            "Research gaps and structural synthesis",
                            "Expose critical gaps and synthesize how current boundaries necessitate this study.",
                            1_200,
                            3,
                        ),
                    ],
                ),
                section_blueprint(
                    "Methodology",
                    "Write a highly robust methodology chapter justifying the epistemological stance, exact research framework, and boundary limitations using rigorous academic conventions.",
                    2_200,
                    "Anchor methodological choices to established scientific literature.",
                    5,
                    vec![
                        section_part(
                            "Epistemological stance and research design",
                            "Defend the core research philosophy and structural design paradigm.",
                            550,
                            1,
                        ),
                        section_part(
                            "Source aggregation and sampling parameters",
                            "Define strict parameters for source selection, sampling scope, and inclusion logic.",
                            550,
                            2,
                        ),
                        section_part(
                            "Analytical processing and synthesis techniques",
                            "Mechanically detail the analytical operations applied to the literature and data.",
                            550,
                            1,
                        ),
                        section_part(
                            "Validity, triangulation, and study boundaries",
                            "Address methodological rigor, ethical limits, and structural boundaries of the study.",
                            550,
                            1,
                        ),
                    ],
                ),
                section_blueprint(
                    "Discussion",
                    "Produce an advanced analytical chapter where raw findings are abstracted into high-level theoretical insights, integrating rigorous academic critique.",
                    5_100,
                    "Do not merely summarize; abstract findings back up to the theoretical frame.",
                    10,
                    vec![
                        section_part(
                            "Synthesis of primary findings",
                            "Translate raw empirical data into clear vectors of academic discovery.",
                            1_300,
                            2,
                        ),
                        section_part(
                            "Theoretical significance of discovered patterns",
                            "Examine causal mechanisms and abstract the empirical findings into theoretical domains.",
                            1_300,
                            3,
                        ),
                        section_part(
                            "Critical implications and disciplinary friction",
                            "Surface profound theoretical contradictions and practical implications confronting the discipline.",
                            1_300,
                            3,
                        ),
                        section_part(
                            "Integrative chapter synthesis",
                            "Close with a sophisticated macro-synthesis anchoring the discussion back to the original thesis.",
                            1_200,
                            2,
                        ),
                    ],
                ),
                section_blueprint(
                    "Conclusion",
                    "Conclude the project by delivering an authoritative, highly condensed final synthesis. Do not introduce new evidence.",
                    900,
                    "Keep the ending highly concise and intellectually definitive.",
                    1,
                    vec![
                        section_part(
                            "Synthesis of primary conclusions",
                            "A deeply academic synthesis of the primary discoveries, weighing their ultimate theoretical and practical significance.",
                            450,
                            0,
                        ),
                        section_part(
                            "Strategic recommendations and final reflection",
                            "Actionable recommendations derived from the data, acknowledging constraints, ending with a definitive final academic reflection.",
                            450,
                            1,
                        ),
                    ],
                ),
            ],
        },
    }
}

async fn generate_research_plan(
    google_key: &str,
    model: &str,
    topic: &str,
    tier: &ResearchTier,
    reference_style: &ReferenceStyle,
    spec: &DocumentSpec,
    sources: &[ResearchSource],
) -> Result<ResearchPlan, String> {
    let prompt = format!(
        "Prepare a plan for a {deliverable} on the topic below.\n\n\
         Topic: {topic}\n\
         Tier: {tier_label}\n\
         Reference style: {reference_style}\n\
         Citation target: {citation_target}\n\
         Minimum word count: {minimum_words}\n\n\
         Evidence dossier:\n{source_dossier}\n\n\
         Return ONLY valid JSON with these keys and no markdown:\n\
         {{\n\
           \"title\": \"specific scholarly title\",\n\
           \"thesis\": \"one-sentence central argument\",\n\
           \"methodology\": \"2-3 sentence methodology statement\",\n\
           \"research_questions\": [\"3 to 5 concise questions\"],\n\
           \"keywords\": [\"5 to 8 keywords\"],\n\
           \"writing_guidance\": \"short paragraph explaining the argumentative arc and how to use the evidence\"\n\
         }}",
        deliverable = spec.deliverable,
        topic = topic,
        tier_label = tier.label(),
        reference_style = reference_style.label(),
        citation_target = tier.citation_target(),
        minimum_words = spec.minimum_words,
        source_dossier = render_source_dossier(sources),
    );

    let raw =
        call_google_model_json(google_key, model, PROFESSOR_SYSTEM_PROMPT, &prompt, 1_600).await?;
    let json = extract_json_object(&raw)?;

    serde_json::from_str::<ResearchPlan>(&json)
        .map_err(|error| format!("Failed to parse planning JSON: {error}"))
}

fn fallback_research_plan(topic: &str, tier: &ResearchTier, spec: &DocumentSpec) -> ResearchPlan {
    ResearchPlan {
        title: format!("A Research Study on {}", topic.trim()),
        thesis: format!(
            "This {} argues that {} should be examined through a rigorous synthesis of current evidence, competing interpretations, and context-specific implications.",
            spec.deliverable,
            topic.trim()
        ),
        methodology: "This document adopts a structured qualitative synthesis of the retrieved evidence, using comparative reading and thematic analysis to identify recurring claims, tensions, and implications.".to_string(),
        research_questions: vec![
            format!("What is the core problem or opportunity embedded in {}?", topic.trim()),
            "How do the retrieved sources converge or diverge in their interpretation of the issue?".to_string(),
            "What practical and scholarly implications emerge from the available evidence?".to_string(),
        ],
        keywords: vec![
            topic.trim().to_string(),
            tier.label().to_string(),
            "evidence synthesis".to_string(),
            "academic analysis".to_string(),
            "research writing".to_string(),
        ],
        writing_guidance: "Move from context to critique, then to evidence-led interpretation and grounded recommendations. Keep the prose formal, cohesive, and citation-aware.".to_string(),
    }
}

fn section_blueprint(
    heading: &'static str,
    brief: &'static str,
    target_words: usize,
    citation_expectation: &'static str,
    minimum_citations: usize,
    parts: Vec<SectionPartBlueprint>,
) -> SectionBlueprint {
    SectionBlueprint {
        heading,
        brief,
        target_words,
        citation_expectation,
        minimum_citations,
        parts,
    }
}

fn section_part(
    label: &'static str,
    brief: &'static str,
    target_words: usize,
    minimum_citations: usize,
) -> SectionPartBlueprint {
    SectionPartBlueprint {
        label,
        brief,
        target_words,
        minimum_citations,
    }
}

async fn generate_section(
    google_key: &str,
    model: &str,
    context: SectionGenerationContext<'_>,
    blueprint: &SectionBlueprint,
) -> Result<ResearchSection, String> {
    let seeded_draft = generate_section_seed_draft(google_key, model, context, blueprint).await?;
    let mut working_draft: Option<String> = Some(convert_outline_section_to_prose(
        &normalize_section_markdown(&seeded_draft, blueprint.heading),
        blueprint.heading,
    ));
    let mut last_failure = String::new();

    for attempt in 0..MAX_SECTION_DRAFT_ATTEMPTS {
        let final_content = if attempt == 0 {
            convert_outline_section_to_prose(
                working_draft
                    .as_deref()
                    .ok_or_else(|| "Missing seeded draft for section review".to_string())?,
                blueprint.heading,
            )
        } else {
            let current_draft = working_draft
                .as_deref()
                .ok_or_else(|| "Missing working draft for section revision".to_string())?;
            let prompt =
                build_section_revision_prompt(context, blueprint, current_draft, &last_failure);
            let raw_draft = call_google_model(
                google_key,
                model,
                PROFESSOR_SYSTEM_PROMPT,
                &prompt,
                0.2,
                section_token_budget(blueprint),
            )
            .await?;
            convert_outline_section_to_prose(
                &normalize_section_markdown(&raw_draft, blueprint.heading),
                blueprint.heading,
            )
        };
        let assessment = evaluate_section_quality(&final_content, blueprint);

        if assessment.passes() {
            return Ok(ResearchSection {
                heading: blueprint.heading.to_string(),
                target_words: blueprint.target_words,
                actual_words: count_words(&final_content),
                content: final_content,
            });
        }

        last_failure = assessment.failure_summary();
        tracing::warn!(
            "Section '{}' rejected on attempt {}/{}: {}",
            blueprint.heading,
            attempt + 1,
            MAX_SECTION_DRAFT_ATTEMPTS,
            last_failure
        );
        working_draft = Some(final_content);
    }

    Err(format!(
        "Section '{}' failed quality checks after {} attempts: {}",
        blueprint.heading, MAX_SECTION_DRAFT_ATTEMPTS, last_failure
    ))
}

async fn generate_section_seed_draft(
    google_key: &str,
    model: &str,
    context: SectionGenerationContext<'_>,
    blueprint: &SectionBlueprint,
) -> Result<String, String> {
    let mut parts = Vec::new();

    for part in &blueprint.parts {
        let generated_part =
            generate_section_part(google_key, model, context, blueprint, part, &parts).await?;
        parts.push(generated_part);
    }

    Ok(assemble_section_from_parts(blueprint.heading, &parts))
}

async fn generate_section_part(
    google_key: &str,
    model: &str,
    context: SectionGenerationContext<'_>,
    blueprint: &SectionBlueprint,
    part: &SectionPartBlueprint,
    completed_parts: &[String],
) -> Result<String, String> {
    let mut working_draft: Option<String> = None;
    let mut last_failure = String::new();

    for attempt in 0..MAX_SECTION_PART_ATTEMPTS {
        let prompt = match working_draft.as_deref() {
            Some(draft) => build_section_part_revision_prompt(
                context,
                blueprint,
                part,
                completed_parts,
                draft,
                &last_failure,
            ),
            None => build_section_part_prompt(context, blueprint, part, completed_parts),
        };

        let raw_draft = call_google_model(
            google_key,
            model,
            PROFESSOR_SYSTEM_PROMPT,
            &prompt,
            if working_draft.is_some() { 0.18 } else { 0.3 },
            section_part_token_budget(part),
        )
        .await?;
        let normalized_draft =
            convert_outline_fragment_to_prose(&normalize_section_part(raw_draft.as_str()));
        let reviewed_draft = review_section_part_for_publication(
            google_key,
            model,
            context,
            blueprint,
            part,
            &normalized_draft,
        )
        .await?;
        let final_content =
            convert_outline_fragment_to_prose(&normalize_section_part(reviewed_draft.as_str()));
        let assessment = evaluate_section_part_quality(&final_content, part);

        if assessment.passes() {
            return Ok(final_content);
        }

        last_failure = assessment.failure_summary();
        tracing::warn!(
            "Section '{}' fragment '{}' rejected on attempt {}/{}: {}",
            blueprint.heading,
            part.label,
            attempt + 1,
            MAX_SECTION_PART_ATTEMPTS,
            last_failure
        );
        working_draft = Some(final_content);
    }

    Err(format!(
        "Section '{}' fragment '{}' failed quality checks after {} attempts: {}",
        blueprint.heading, part.label, MAX_SECTION_PART_ATTEMPTS, last_failure
    ))
}

fn build_section_part_prompt(
    context: SectionGenerationContext<'_>,
    blueprint: &SectionBlueprint,
    part: &SectionPartBlueprint,
    completed_parts: &[String],
) -> String {
    format!(
        "Write only the next polished prose fragment for a larger academic section.\n\n\
         Topic: {topic}\n\
         Deliverable: {deliverable}\n\
         Title: {title}\n\
         Tier: {tier_label}\n\
         Reference style: {reference_style}\n\
         Central thesis: {thesis}\n\
         Methodology frame: {methodology}\n\
         Keywords: {keywords}\n\
         Parent section heading: {heading}\n\
         Parent section brief: {section_brief}\n\
         Fragment focus: {part_label}\n\
         Fragment brief: {part_brief}\n\
         Target fragment length: about {target_words} words\n\
         Minimum acceptable fragment length: {minimum_words} words\n\
         Minimum in-text citations in this fragment: {minimum_citations}\n\
         Citation guidance: {citation_expectation}\n\n\
         Research questions:\n{research_questions}\n\n\
         Previous section continuity notes:\n{continuity}\n\n\
         Earlier fragments already drafted for this same section:\n{prior_fragments}\n\n\
         Evidence dossier to use silently:\n{source_dossier}\n\n\
         ⚠️ HARD REQUIREMENTS (violation = failure):\n\
         - Begin your response with the VERY FIRST SENTENCE of finished academic prose. Not a plan. Not a label. Not a check.\n\
         - NEVER output ANY of the following: outlines, planning notes, self-checks, word count estimates, quality checks, revision thoughts, constraint checks, labels like 'Opening:', 'Body:', 'Check:', 'Draft:', or any meta-commentary whatsoever\n\
         - Return ONLY connected academic prose paragraphs for this fragment\n\
         - Do not include the main section heading, a subsection heading, or labels such as \"Fragment 1\"\n\
         - Advance the argument from the earlier fragments instead of repeating their setup\n\
         - Write only polished paragraphs in final scholarly prose\n\
         - Do not echo the brief, source list, or planning memo\n\
         - Do not use bullets, numbering, checklists, or labels like \"Drafting:\", \"Paragraph 1:\", or \"Word count check:\"\n\
         - Do not include editor notes, self-evaluation, or meta commentary\n\
         - Use the requested reference style for in-text citations\n\
         - Return only the completed fragment",
        topic = context.topic,
        deliverable = context.tier.deliverable_name(),
        title = context.plan.title,
        tier_label = context.tier.label(),
        reference_style = context.reference_style.label(),
        thesis = context.plan.thesis,
        methodology = context.plan.methodology,
        keywords = context.plan.keywords.join(", "),
        heading = blueprint.heading,
        section_brief = blueprint.brief,
        part_label = part.label,
        part_brief = part.brief,
        target_words = part.target_words,
        minimum_words = minimum_words_for_section_part(part),
        minimum_citations = part.minimum_citations,
        citation_expectation = blueprint.citation_expectation,
        research_questions = render_research_questions(&context.plan.research_questions),
        continuity = render_previous_sections(context.previous_sections),
        prior_fragments = render_section_part_previews(completed_parts),
        source_dossier = render_source_dossier(context.sources),
    )
}

fn build_section_part_revision_prompt(
    context: SectionGenerationContext<'_>,
    blueprint: &SectionBlueprint,
    part: &SectionPartBlueprint,
    completed_parts: &[String],
    prior_draft: &str,
    failure_summary: &str,
) -> String {
    format!(
        "Rewrite the prose fragment below so it can be stitched into a final academic section.\n\n\
         Topic: {topic}\n\
         Deliverable: {deliverable}\n\
         Title: {title}\n\
         Tier: {tier_label}\n\
         Reference style: {reference_style}\n\
         Parent section heading: {heading}\n\
         Fragment focus: {part_label}\n\
         Fragment brief: {part_brief}\n\
         Minimum acceptable fragment length: {minimum_words} words\n\
         Minimum in-text citations in this fragment: {minimum_citations}\n\
         Problems found in the previous fragment draft: {failure_summary}\n\n\
         Earlier fragments already drafted for this same section:\n{prior_fragments}\n\n\
         Evidence dossier to use silently:\n{source_dossier}\n\n\
         Previous fragment draft to fix:\n{prior_draft}\n\n\
         Hard requirements:\n\
         - Return only corrected academic prose paragraphs for this fragment\n\
         - Do not include headings, bullets, numbering, labels, editor notes, or prompt echoes\n\
         - Convert every outline fragment into full prose and smooth the transition from earlier fragments\n\
         - Expand the fragment until it meets the length requirement\n\
         - Use the requested in-text citation style and add enough citations to meet the minimum\n\
         - Return only the corrected fragment",
        topic = context.topic,
        deliverable = context.tier.deliverable_name(),
        title = context.plan.title,
        tier_label = context.tier.label(),
        reference_style = context.reference_style.label(),
        heading = blueprint.heading,
        part_label = part.label,
        part_brief = part.brief,
        minimum_words = minimum_words_for_section_part(part),
        minimum_citations = part.minimum_citations,
        failure_summary = if failure_summary.is_empty() {
            "The fragment did not satisfy the quality bar."
        } else {
            failure_summary
        },
        prior_fragments = render_section_part_previews(completed_parts),
        source_dossier = render_source_dossier(context.sources),
        prior_draft = prior_draft,
    )
}

fn build_section_revision_prompt(
    context: SectionGenerationContext<'_>,
    blueprint: &SectionBlueprint,
    prior_draft: &str,
    failure_summary: &str,
) -> String {
    format!(
        "Rewrite the draft below into a publishable academic section.\n\n\
         Topic: {topic}\n\
         Deliverable: {deliverable}\n\
         Title: {title}\n\
         Tier: {tier_label}\n\
         Reference style: {reference_style}\n\
         Section heading: {heading}\n\
         Minimum acceptable length: {minimum_words} words\n\
         Minimum in-text citations: {minimum_citations}\n\
         Problems found in the previous draft: {failure_summary}\n\n\
         Evidence dossier to use silently:\n{source_dossier}\n\n\
         Previous draft to fix:\n{prior_draft}\n\n\
         Hard requirements:\n\
         - Keep the heading as \"## {heading}\"\n\
         - Smooth any seams or repetition created by stitching together shorter draft fragments\n\
         - Convert every outline fragment into full academic prose\n\
         - Remove bullets, numbering, labels, editorial notes, and prompt echoes\n\
         - Expand the content until it meets the length requirement\n\
         - Use the requested in-text citation style and add enough citations to meet the minimum\n\
         - Return only the corrected section",
        topic = context.topic,
        deliverable = context.tier.deliverable_name(),
        title = context.plan.title,
        tier_label = context.tier.label(),
        reference_style = context.reference_style.label(),
        heading = blueprint.heading,
        minimum_words = minimum_words_for_section(blueprint),
        minimum_citations = blueprint.minimum_citations,
        failure_summary = if failure_summary.is_empty() {
            "The draft did not satisfy the quality bar."
        } else {
            failure_summary
        },
        source_dossier = render_source_dossier(context.sources),
        prior_draft = prior_draft,
    )
}

async fn review_section_for_publication(
    google_key: &str,
    model: &str,
    context: SectionGenerationContext<'_>,
    blueprint: &SectionBlueprint,
    draft: &str,
) -> Result<String, String> {
    let prompt = format!(
        "Act as a strict academic editor. Clean and strengthen the section below before publication.\n\n\
         Topic: {topic}\n\
         Title: {title}\n\
         Reference style: {reference_style}\n\
         Section heading: {heading}\n\
         Minimum acceptable length: {minimum_words} words\n\
         Minimum in-text citations: {minimum_citations}\n\n\
         Draft:\n{draft}\n\n\
         Editorial goals:\n\
         - Preserve the intended argument while rewriting weak or outline-like material into full paragraphs\n\
         - Smooth transitions so the section reads as one continuous finished chapter rather than stitched fragments\n\
         - Remove any bullets, numbering, labels, duplicated prompts, or self-referential text\n\
         - Ensure the output reads like final academic prose, not notes\n\
         - Keep the heading as \"## {heading}\"\n\
         - Return only the cleaned section",
        topic = context.topic,
        title = context.plan.title,
        reference_style = context.reference_style.label(),
        heading = blueprint.heading,
        minimum_words = minimum_words_for_section(blueprint),
        minimum_citations = blueprint.minimum_citations,
        draft = draft,
    );

    call_google_model(
        google_key,
        model,
        PROFESSOR_SYSTEM_PROMPT,
        &prompt,
        0.15,
        section_token_budget(blueprint),
    )
    .await
}

async fn review_section_part_for_publication(
    google_key: &str,
    model: &str,
    context: SectionGenerationContext<'_>,
    blueprint: &SectionBlueprint,
    part: &SectionPartBlueprint,
    draft: &str,
) -> Result<String, String> {
    let prompt = format!(
        "Act as a strict academic editor. Clean and strengthen the prose fragment below before it is stitched into a larger section.\n\n\
         Topic: {topic}\n\
         Title: {title}\n\
         Reference style: {reference_style}\n\
         Parent section heading: {heading}\n\
         Fragment focus: {part_label}\n\
         Minimum acceptable fragment length: {minimum_words} words\n\
         Minimum in-text citations in this fragment: {minimum_citations}\n\n\
         Draft:\n{draft}\n\n\
         Editorial goals:\n\
         - Preserve the argument while rewriting any outline-like material into full paragraph prose\n\
         - Remove bullets, numbering, headings, labels, duplicated prompts, and self-referential text\n\
         - Ensure the output reads like final academic prose that can slot into the larger section without obvious seams\n\
         - Return only the cleaned prose fragment",
        topic = context.topic,
        title = context.plan.title,
        reference_style = context.reference_style.label(),
        heading = blueprint.heading,
        part_label = part.label,
        minimum_words = minimum_words_for_section_part(part),
        minimum_citations = part.minimum_citations,
        draft = draft,
    );

    call_google_model(
        google_key,
        model,
        PROFESSOR_SYSTEM_PROMPT,
        &prompt,
        0.15,
        section_part_token_budget(part),
    )
    .await
}

async fn revise_report_sections(
    google_key: &str,
    model: &str,
    report_context: ReportContext<'_>,
    sections: &[ResearchSection],
    quality: &ResearchQualityReport,
) -> Result<Vec<ResearchSection>, String> {
    let revision_targets = identify_sections_for_revision(sections, report_context.spec, quality);
    let mut revised_sections = Vec::new();

    for (index, blueprint) in report_context.spec.section_blueprints.iter().enumerate() {
        if revision_targets.contains(&index) {
            let existing = sections
                .get(index)
                .ok_or_else(|| format!("Missing section at index {index} during revision"))?;
            let context = SectionGenerationContext {
                topic: report_context.topic,
                tier: report_context.tier,
                reference_style: report_context.reference_style,
                plan: report_context.plan,
                sources: report_context.sources,
                previous_sections: &revised_sections,
            };
            let failure_summary = summarize_section_revision_need(existing, blueprint, quality);
            let rewritten = build_section_revision_prompt(
                context,
                blueprint,
                &existing.content,
                &failure_summary,
            );
            let raw = call_google_model(
                google_key,
                model,
                PROFESSOR_SYSTEM_PROMPT,
                &rewritten,
                0.2,
                section_token_budget(blueprint),
            )
            .await?;
            let cleaned = review_section_for_publication(
                google_key,
                model,
                context,
                blueprint,
                &normalize_section_markdown(&raw, blueprint.heading),
            )
            .await?;
            revised_sections.push(ResearchSection {
                heading: blueprint.heading.to_string(),
                target_words: blueprint.target_words,
                actual_words: count_words(&cleaned),
                content: normalize_section_markdown(&cleaned, blueprint.heading),
            });
        } else {
            revised_sections.push(
                sections
                    .get(index)
                    .ok_or_else(|| {
                        format!("Missing section at index {index} during carry-forward")
                    })?
                    .clone(),
            );
        }
    }

    Ok(revised_sections)
}

async fn generate_references_section(
    google_key: &str,
    model: &str,
    report_context: ReportContext<'_>,
    sections: &[ResearchSection],
) -> Result<String, String> {
    let prompt = format!(
        "Compile the references section for the completed {deliverable} below.\n\n\
         Topic: {topic}\n\
         Title: {title}\n\
         Tier: {tier_label}\n\
         Reference style: {reference_style}\n\
         Citation target: {citation_target}\n\n\
         Evidence dossier:\n{source_dossier}\n\n\
         Existing sections:\n{sections_text}\n\n\
         Return only a markdown section beginning with \"## {bibliography_heading}\".\n\
         Use {reference_style} as closely as possible.\n\
         Prefer the provided sources and avoid inventing DOIs or publication details you do not know.\n\
         Each reference should appear on its own paragraph or line.",
        deliverable = report_context.tier.deliverable_name(),
        topic = report_context.topic,
        title = report_context.plan.title,
        tier_label = report_context.tier.label(),
        reference_style = report_context.reference_style.label(),
        bibliography_heading = report_context.reference_style.bibliography_heading(),
        citation_target = report_context.tier.citation_target(),
        source_dossier = render_source_dossier(report_context.sources),
        sections_text = render_existing_sections(sections),
    );

    let raw = call_google_model(
        google_key,
        model,
        PROFESSOR_SYSTEM_PROMPT,
        &prompt,
        0.2,
        2_048,
    )
    .await?;
    Ok(normalize_references_section(
        &raw,
        report_context.reference_style,
    ))
}

fn assemble_report_markdown(
    report_context: ReportContext<'_>,
    sections: &[ResearchSection],
    references: &str,
) -> String {
    let mut markdown = String::new();
    markdown.push_str(&format!("# {}\n\n", report_context.plan.title.trim()));
    markdown.push_str(&format!("Topic: {}\n", report_context.topic.trim()));
    markdown.push_str(&format!(
        "Deliverable: {}\n",
        report_context.spec.deliverable
    ));
    markdown.push_str(&format!("Tier: {}\n", report_context.tier.label()));
    markdown.push_str(&format!(
        "Reference style: {}\n",
        report_context.reference_style.label()
    ));
    markdown.push_str(&format!(
        "Citation target: {}\n",
        report_context.tier.citation_target()
    ));
    markdown.push_str(&format!(
        "Keywords: {}\n\n",
        report_context.plan.keywords.join(", ")
    ));

    for section in sections {
        markdown.push_str(section.content.trim());
        markdown.push_str("\n\n");
    }

    markdown.push_str(references.trim());
    markdown.push('\n');

    if report_context.spec.include_source_matrix && !report_context.sources.is_empty() {
        markdown.push('\n');
        markdown.push_str(&build_source_matrix_section(report_context.sources));
    }

    markdown
}

fn build_source_matrix_section(sources: &[ResearchSource]) -> String {
    let mut appendix = String::from("## Appendix A - Source Matrix\n\n");

    for (index, source) in sources.iter().enumerate() {
        let year_label = source
            .year_hint
            .map(|year| year.to_string())
            .unwrap_or_else(|| "n.d.".to_string());
        let citation_key = derive_citation_key(source);
        appendix.push_str(&format!(
            "Material {}: {} ({})\nType: {}\nLocator: {}\nEvidence note: {}\nSuggested citation: {}\n\n",
            index + 1,
            source.title,
            year_label,
            source.source_type,
            if source.url.is_empty() { "No URL captured" } else { &source.url },
            source.summary,
            citation_key

        ));
    }

    appendix
}

fn analyze_report_quality(
    markdown: &str,
    spec: &DocumentSpec,
    citation_target: usize,
    reference_style: &ReferenceStyle,
) -> ResearchQualityReport {
    let narrative_markdown = report_body_for_quality(markdown, reference_style);
    let missing_headings = spec
        .section_blueprints
        .iter()
        .filter_map(|blueprint| {
            let heading = format!("## {}", blueprint.heading);
            if markdown.contains(&heading) {
                None
            } else {
                Some(blueprint.heading.to_string())
            }
        })
        .collect::<Vec<_>>();
    let banned_phrase_hits = collect_banned_phrase_hits(narrative_markdown);
    let bullet_free = narrative_markdown.lines().all(|line| !is_bullet_line(line));
    let prose_only = bullet_free && banned_phrase_hits.is_empty();
    let citation_mentions = count_citation_mentions(narrative_markdown);
    let actual_word_count = count_words(narrative_markdown);

    ResearchQualityReport {
        required_headings_present: missing_headings.is_empty(),
        references_present: markdown
            .contains(&format!("## {}", reference_style.bibliography_heading())),
        bullet_free,
        prose_only,
        minimum_word_count_met: actual_word_count
            >= (spec.minimum_words * SECTION_WORD_FLOOR_PERCENT) / 100,
        citation_target_met: citation_mentions >= citation_target,
        actual_word_count,
        minimum_word_count: (spec.minimum_words * SECTION_WORD_FLOOR_PERCENT) / 100,
        citation_mentions,
        citation_target,
        missing_headings,
        banned_phrase_hits,
    }
}

fn report_body_for_quality<'a>(markdown: &'a str, reference_style: &ReferenceStyle) -> &'a str {
    let bibliography_heading = format!("## {}", reference_style.bibliography_heading());
    markdown
        .split_once(&bibliography_heading)
        .map(|(body, _)| body.trim_end())
        .unwrap_or(markdown)
}

async fn persist_report_artifacts(report: &ResearchReport) -> Result<ResearchArtifacts, String> {
    let output_dir = output_dir();
    fs::create_dir_all(&output_dir)
        .await
        .map_err(|error| format!("Failed to create output directory: {error}"))?;

    let timestamp = Utc::now().format("%Y%m%dT%H%M%SZ");
    let slug = slugify(&report.title);
    let base_name = format!("{}-{}", timestamp, slug);
    let markdown_path = output_dir.join(format!("{base_name}.md"));
    let json_path = output_dir.join(format!("{base_name}.json"));
    let pdf_path = output_dir.join(format!("{base_name}.pdf"));

    fs::write(&markdown_path, &report.markdown)
        .await
        .map_err(|error| format!("Failed to write markdown artifact: {error}"))?;

    let artifacts = ResearchArtifacts {
        markdown_path: Some(markdown_path.display().to_string()),
        json_path: Some(json_path.display().to_string()),
        pdf_path: Some(pdf_path.display().to_string()),
    };

    let mut persisted_report = report.clone();
    persisted_report.artifacts = Some(artifacts.clone());
    let payload = serde_json::to_vec_pretty(&persisted_report)
        .map_err(|error| format!("Failed to serialize report JSON: {error}"))?;

    fs::write(&json_path, payload)
        .await
        .map_err(|error| format!("Failed to write JSON artifact: {error}"))?;

    generate_pdf_artifact(&json_path).await?;

    Ok(artifacts)
}

fn output_dir() -> PathBuf {
    env::var("RESEARCH_BOT_OUTPUT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("papers"))
}

async fn generate_pdf_artifact(json_path: &PathBuf) -> Result<(), String> {
    let script_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("scripts")
        .join("generate_pdf.py");

    let output = Command::new("python3")
        .arg(&script_path)
        .arg(json_path)
        .output()
        .await
        .map_err(|error| format!("Failed to start PDF generation: {error}"))?;

    if !output.status.success() {
        return Err(format!(
            "PDF generation failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }

    Ok(())
}

async fn call_google_model(
    google_key: &str,
    model: &str,
    system_prompt: &str,
    user_prompt: &str,
    temperature: f32,
    max_output_tokens: u32,
) -> Result<String, String> {
    if google_key.is_empty() {
        return Err("GOOGLE_API_KEY is empty".to_string());
    }

    let body = serde_json::json!({
        "contents": [
            {
                "role": "user",
                "parts": [{ "text": user_prompt }]
            }
        ],
        "systemInstruction": {
            "parts": [{ "text": system_prompt }]
        },
        "generationConfig": {
            "temperature": temperature,
            "maxOutputTokens": max_output_tokens
        }
    });

    let url = format!(
        "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={google_key}"
    );

    let client = reqwest::Client::new();
    let max_retries = 3u32;
    let mut last_error = String::new();

    let mut rate_limit_hint: Option<u64> = None;
    for attempt in 0..=max_retries {
        if attempt > 0 {
            let base_delay = rate_limit_hint.take().unwrap_or(30 * 2u64.pow(attempt - 1));
            let delay = std::time::Duration::from_secs(base_delay);
            tracing::warn!(
                "🔄 Retrying API call (attempt {}/{}) after {}s backoff...",
                attempt + 1,
                max_retries + 1,
                delay.as_secs()
            );
            tokio::time::sleep(delay).await;
        }

        let response = match client
            .post(&url)
            .header("content-type", "application/json")
            .json(&body)
            .send()
            .await
        {
            Ok(r) => r,
            Err(error) => {
                last_error = format!("Google Generative Language request failed: {error}");
                if attempt < max_retries {
                    continue;
                }
                return Err(last_error);
            }
        };

        let status = response.status();
        let json: serde_json::Value = response
            .json()
            .await
            .map_err(|error| format!("Failed to parse model response: {error}"))?;

        if !status.is_success() {
            let err_msg = json
                .get("error")
                .and_then(|error| error.get("message"))
                .and_then(|message| message.as_str())
                .unwrap_or("Unknown error");

            last_error = format!("Model API error ({}): {}", status, err_msg);

            if should_retry_model_status(status, err_msg) && attempt < max_retries {
                rate_limit_hint = extract_retry_delay_secs(err_msg);
                continue;
            }

            return Err(last_error);
        }

        return json["candidates"]
            .as_array()
            .and_then(|candidates| candidates.first())
            .and_then(|candidate| candidate["content"]["parts"].as_array())
            .and_then(|parts| {
                parts.iter().find_map(|part| {
                    if part
                        .get("thought")
                        .and_then(|t| t.as_bool())
                        .unwrap_or(false)
                    {
                        None
                    } else {
                        part["text"].as_str().map(str::to_string)
                    }
                })
            })
            .map(|text| strip_code_fences(&text))
            .ok_or_else(|| "No text content in model response".to_string());
    }

    Err(last_error)
}

async fn call_google_model_json(
    google_key: &str,
    model: &str,
    system_prompt: &str,
    user_prompt: &str,
    max_output_tokens: u32,
) -> Result<String, String> {
    if google_key.is_empty() {
        return Err("GOOGLE_API_KEY is empty".to_string());
    }

    let body = serde_json::json!({
        "contents": [
            {
                "role": "user",
                "parts": [{ "text": user_prompt }]
            }
        ],
        "systemInstruction": {
            "parts": [{ "text": system_prompt }]
        },
        "generationConfig": {
            "temperature": 0.1,
            "maxOutputTokens": max_output_tokens,
            "responseMimeType": "application/json"
        }
    });

    let url = format!(
        "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={google_key}"
    );

    let client = reqwest::Client::new();
    let max_retries = 3u32;
    let mut last_error = String::new();

    for attempt in 0..=max_retries {
        if attempt > 0 {
            let delay = std::time::Duration::from_secs(30 * 2u64.pow(attempt - 1));
            tracing::warn!(
                "🔄 Retrying JSON planning call (attempt {}/{}) after {}s backoff...",
                attempt + 1,
                max_retries + 1,
                delay.as_secs()
            );
            tokio::time::sleep(delay).await;
        }

        let response = match client
            .post(&url)
            .header("content-type", "application/json")
            .json(&body)
            .send()
            .await
        {
            Ok(r) => r,
            Err(error) => {
                last_error = format!("Google JSON planning request failed: {error}");
                if attempt < max_retries {
                    continue;
                }
                return Err(last_error);
            }
        };

        let status = response.status();
        let json: serde_json::Value = response
            .json()
            .await
            .map_err(|error| format!("Failed to parse JSON planning response: {error}"))?;

        if !status.is_success() {
            let err_msg = json
                .get("error")
                .and_then(|error| error.get("message"))
                .and_then(|message| message.as_str())
                .unwrap_or("Unknown error");

            last_error = format!("Model JSON planning error ({}): {}", status, err_msg);

            if should_retry_model_status(status, err_msg) && attempt < max_retries {
                continue;
            }

            return Err(last_error);
        }

        return json["candidates"]
            .as_array()
            .and_then(|candidates| candidates.first())
            .and_then(|candidate| candidate["content"]["parts"].as_array())
            .and_then(|parts| {
                parts.iter().find_map(|part| {
                    if part
                        .get("thought")
                        .and_then(|t| t.as_bool())
                        .unwrap_or(false)
                    {
                        None
                    } else {
                        part["text"].as_str().map(str::to_string)
                    }
                })
            })
            .ok_or_else(|| "No JSON text content in planning response".to_string());
    }

    Err(last_error)
}

/// Derive a short APA-style citation key from real source metadata.
/// Produces something like "BBC News, 2023" or "pmc.ncbi.nlm.nih.gov, 2021".
fn derive_citation_key(source: &ResearchSource) -> String {
    // Clean up the author label — strip URL residue if it looks like a domain
    let author_clean = {
        let a = source.author_or_channel.trim();
        // If it looks like a domain (contains dots but no spaces), present as-is
        // Otherwise capitalise it nicely
        if a.contains('.') && !a.contains(' ') {
            // Try to get a readable name from common domains
            let readable = match a {
                s if s.contains("ncbi.nlm.nih.gov") || s.contains("pmc.ncbi.nlm.nih.gov") => {
                    "National Institutes of Health"
                }
                s if s.contains("bbc.") => "BBC News",
                s if s.contains("cnn.") => "CNN",
                s if s.contains("nature.com") => "Nature",
                s if s.contains("sciencedirect") => "ScienceDirect",
                s if s.contains("mdpi.com") => "MDPI",
                s if s.contains("who.int") => "World Health Organization",
                s if s.contains("nih.gov") => "National Institutes of Health",
                s if s.contains("cdc.gov") => "Centers for Disease Control",
                s if s.contains("pubmed") => "PubMed",
                s if s.contains("academia.edu") => "Academia",
                s if s.contains("researchgate") => "ResearchGate",
                s if s.contains("springer") => "Springer",
                s if s.contains("wiley") => "Wiley",
                s if s.contains("tandfonline") => "Taylor & Francis",
                s if s.contains("jstor") => "JSTOR",
                s if s.contains("sagepub") => "SAGE Publications",
                s if s.contains("elsevier") => "Elsevier",
                s if s.contains("harvard") => "Harvard University",
                s if s.contains("oxford") => "Oxford University Press",
                s if s.contains("cambridge") => "Cambridge University Press",
                s if s.contains("worldbank") => "World Bank",
                s if s.contains("imf.org") => "International Monetary Fund",
                s if s.contains("un.org") => "United Nations",
                s if s.contains("youtube") => "YouTube",
                _ => a,
            };
            readable.to_string()
        } else {
            a.to_string()
        }
    };

    let year = source
        .year_hint
        .map(|y| y.to_string())
        .unwrap_or_else(|| "n.d.".to_string());

    format!("{}, {}", author_clean, year)
}

fn render_source_dossier(sources: &[ResearchSource]) -> String {
    if sources.is_empty() {
        return "No live sources were retrieved. Use conservative, well-established background knowledge and clearly qualify uncertainty.".to_string();
    }

    let mut dossier = String::new();
    dossier.push_str(
        "CITATION RULES (non-negotiable):\n\
         - You MUST cite sources using ONLY the citation keys listed below.\n\
         - Do NOT invent author names, years, or citation labels not listed here.\n\
         - If a source does not have a clear author, use the provided key exactly as written.\n\
         - Format every in-text citation exactly as shown in the key, e.g. (National Institutes of Health, 2021).\n\n",
    );

    dossier.push_str("AVAILABLE CITATION KEYS (use these verbatim in your prose):\n");
    for (index, source) in sources.iter().enumerate() {
        let key = derive_citation_key(source);
        dossier.push_str(&format!("  [{}] ({})\n", index + 1, key));
    }
    dossier.push('\n');

    dossier.push_str("SOURCE EVIDENCE DOSSIER:\n");
    for (index, source) in sources.iter().enumerate() {
        let key = derive_citation_key(source);
        let year_label = source
            .year_hint
            .map(|year| year.to_string())
            .unwrap_or_else(|| "n.d.".to_string());
        dossier.push_str(&format!(
            "Source {} — cite as ({})\nTitle: {}\nURL: {}\nYear: {}\nEvidence summary: {}\n\n",
            index + 1,
            key,
            source.title,
            if source.url.is_empty() {
                "N/A"
            } else {
                &source.url
            },
            year_label,
            source.summary,
        ));
    }
    dossier
}

fn render_whatsapp_source_packets(sources: &[ResearchSource]) -> String {
    if sources.is_empty() {
        return "No live sources were available. Give a careful, uncertainty-aware answer and say the evidence base is thin.".to_string();
    }

    let mut packets = String::new();
    for (index, source) in sources.iter().take(14).enumerate() {
        packets.push_str(&format!(
            "[{}] type={} | title={} | source={} | url={} | summary={}\n",
            index + 1,
            source.source_type,
            source.title,
            source.author_or_channel,
            if source.url.is_empty() {
                "N/A"
            } else {
                &source.url
            },
            source.summary.replace('\n', " "),
        ));
    }

    packets
}

fn normalize_whatsapp_answer(text: &str, topic: &str, sources: &[ResearchSource]) -> String {
    let mut answer = strip_code_fences(text).trim().to_string();
    if answer.is_empty() {
        return fallback_whatsapp_brief(topic, sources);
    }

    if answer.len() > 3_400 {
        answer = truncate(&answer, 3_400);
    }

    answer
}

fn fallback_whatsapp_brief(topic: &str, sources: &[ResearchSource]) -> String {
    let top_summaries = sources
        .iter()
        .take(3)
        .map(|source| format!("- {}: {}", source.title, truncate(&source.summary, 180)))
        .collect::<Vec<_>>();
    let source_links = sources
        .iter()
        .take(5)
        .map(|source| {
            if source.url.is_empty() {
                format!("- {} ({})", source.title, source.source_type)
            } else {
                format!("- {} — {}", source.title, source.url)
            }
        })
        .collect::<Vec<_>>();
    let books = sources
        .iter()
        .filter(|source| matches!(source.source_type.as_str(), "book" | "public_domain_book"))
        .take(3)
        .map(|source| {
            if source.url.is_empty() {
                format!("- {} by {}", source.title, source.author_or_channel)
            } else {
                format!(
                    "- {} by {} — {}",
                    source.title, source.author_or_channel, source.url
                )
            }
        })
        .collect::<Vec<_>>();

    let mut brief = format!(
        "Research brief: {topic}\n\nI pulled what I could from web search, YouTube, public discussion, and legal book sources. The strongest immediate signal is below.\n\nWhat stands out:\n{}",
        if top_summaries.is_empty() {
            "- I could not retrieve live sources for this topic, so the answer should be treated cautiously.".to_string()
        } else {
            top_summaries.join("\n")
        }
    );

    if !source_links.is_empty() {
        brief.push_str("\n\nSources to open:\n");
        brief.push_str(&source_links.join("\n"));
    }

    if !books.is_empty() {
        brief.push_str("\n\nBooks:\n");
        brief.push_str(&books.join("\n"));
    }

    brief
}

fn render_research_questions(questions: &[String]) -> String {
    if questions.is_empty() {
        return "What is the central problem, what does the evidence suggest, and what follows from the analysis?"
            .to_string();
    }

    questions.join(" | ")
}

fn render_previous_sections(sections: &[ResearchSection]) -> String {
    if sections.is_empty() {
        return "No previous sections yet. Set the tone and establish a clear foundation."
            .to_string();
    }

    sections
        .iter()
        .map(|section| {
            format!(
                "{}: {}",
                section.heading,
                truncate(&section_body_preview(&section.content, 320), 320)
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn render_section_part_previews(parts: &[String]) -> String {
    if parts.is_empty() {
        return "No earlier fragments yet. Establish a clear starting point for the section."
            .to_string();
    }

    parts
        .iter()
        .enumerate()
        .map(|(index, part)| {
            format!(
                "Earlier prose excerpt {}: {}",
                index + 1,
                truncate(part.trim(), 260)
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn render_existing_sections(sections: &[ResearchSection]) -> String {
    sections
        .iter()
        .map(|section| section.content.clone())
        .collect::<Vec<_>>()
        .join("\n\n")
}

fn assemble_section_from_parts(heading: &str, parts: &[String]) -> String {
    let body = parts
        .iter()
        .map(|part| part.trim())
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("\n\n");

    if body.is_empty() {
        format!("## {heading}")
    } else {
        format!("## {heading}\n\n{body}")
    }
}

fn normalize_section_markdown(raw: &str, heading: &str) -> String {
    let cleaned = strip_code_fences(raw).trim().to_string();
    let expected_heading = format!("## {heading}");

    if cleaned.starts_with(&expected_heading) {
        return cleaned;
    }

    let without_heading = cleaned
        .strip_prefix(heading)
        .map(str::trim_start)
        .or_else(|| {
            cleaned
                .strip_prefix(&format!("# {heading}"))
                .map(str::trim_start)
        })
        .or_else(|| {
            cleaned
                .strip_prefix(&format!("## {heading}"))
                .map(str::trim_start)
        })
        .unwrap_or(cleaned.as_str());

    format!("## {heading}\n\n{}", without_heading.trim())
}

fn normalize_section_part(raw: &str) -> String {
    let cleaned = strip_code_fences(raw);
    let filtered_lines = cleaned
        .lines()
        .filter(|line| {
            let trimmed = line.trim();
            !trimmed.starts_with('#')
                && !trimmed.to_lowercase().starts_with("section:")
                && !trimmed.to_lowercase().starts_with("subsection:")
        })
        .collect::<Vec<_>>();

    let candidate = filtered_lines.join("\n").trim().to_string();
    if candidate.is_empty() {
        cleaned.trim().to_string()
    } else {
        candidate
    }
}

fn convert_outline_fragment_to_prose(text: &str) -> String {
    if count_bullet_lines(text) == 0 && collect_banned_phrase_hits(text).is_empty() {
        return text.trim().to_string();
    }

    let mut paragraphs = Vec::new();
    let mut current_sentences = Vec::new();

    for line in text.lines() {
        let cleaned = clean_outline_line(line);
        if cleaned.is_empty() {
            if !current_sentences.is_empty() {
                paragraphs.push(current_sentences.join(" "));
                current_sentences.clear();
            }
            continue;
        }
        current_sentences.push(cleaned);
    }

    if !current_sentences.is_empty() {
        paragraphs.push(current_sentences.join(" "));
    }

    paragraphs
        .into_iter()
        .filter(|paragraph| !paragraph.trim().is_empty())
        .collect::<Vec<_>>()
        .join("\n\n")
        .trim()
        .to_string()
}

fn convert_outline_section_to_prose(section: &str, heading: &str) -> String {
    let normalized = normalize_section_markdown(section, heading);
    let body = normalized
        .lines()
        .skip_while(|line| line.trim().starts_with("## "))
        .collect::<Vec<_>>()
        .join("\n");
    let cleaned_body = convert_outline_fragment_to_prose(&body);

    if cleaned_body.is_empty() {
        format!("## {heading}")
    } else {
        format!("## {heading}\n\n{cleaned_body}")
    }
}

fn clean_outline_line(line: &str) -> String {
    let without_bullet = strip_bullet_prefix(line.trim());
    let mut cleaned = strip_outline_label(without_bullet).trim().to_string();

    if cleaned.is_empty() {
        return cleaned;
    }

    let lowered = cleaned.to_lowercase();
    if lowered.starts_with("word count")
        || lowered.starts_with("citation count")
        || lowered.starts_with("tone check")
        || lowered.starts_with("draft ready")
        || lowered.contains("drafting thought:")
        || lowered.contains("self-correction")
        || lowered.contains("self correction")
        || lowered.contains("wait, ")
        || lowered.contains("wait:")
    {
        return String::new();
    }

    if !matches!(cleaned.chars().last(), Some('.' | '?' | '!' | ':')) {
        cleaned.push('.');
    }

    cleaned
}

fn strip_bullet_prefix(line: &str) -> &str {
    let trimmed = line.trim_start();

    if let Some(stripped) = trimmed
        .strip_prefix("- ")
        .or_else(|| trimmed.strip_prefix("* "))
        .or_else(|| trimmed.strip_prefix("• "))
    {
        return stripped;
    }

    if is_numbered_list(trimmed) {
        if let Some((_, remainder)) = trimmed.split_once(". ") {
            return remainder;
        }
    }

    trimmed
}

fn strip_outline_label(line: &str) -> String {
    let mut trimmed = line.trim();
    while trimmed.starts_with('*') {
        trimmed = trimmed[1..].trim();
    }

    let lowered = trimmed.to_lowercase();

    for marker in BANNED_META_MARKERS {
        if lowered.starts_with(marker) {
            let mut result = trimmed[marker.len()..].trim();
            while result.starts_with('*') {
                result = result[1..].trim();
            }
            return result.to_string();
        }
    }

    if let Some((label, remainder)) = trimmed.split_once(':') {
        let mut lowered_label = label.trim().to_lowercase();
        while lowered_label.ends_with('*') {
            let len = lowered_label.len();
            lowered_label.truncate(len - 1);
            lowered_label = lowered_label.trim().to_string();
        }

        if lowered_label.starts_with("paragraph ")
            || lowered_label.starts_with("theme ")
            || lowered_label.starts_with("fragment ")
            || lowered_label.starts_with("source ")
            || lowered_label.starts_with("section ")
            || lowered_label.starts_with("subsection ")
            || lowered_label.starts_with("sentence ")
            || lowered_label.starts_with("drafting ")
            || lowered_label.starts_with("wait")
            || lowered_label == "intro"
            || lowered_label == "conclusion"
            || lowered_label == "drafting thought"
            || lowered_label == "self-correction"
            || lowered_label == "self correction"
            || lowered_label == "correction"
            || lowered_label == "actually"
            || lowered_label == "word count check"
        {
            let mut final_remainder = remainder.trim();
            while final_remainder.starts_with('*') {
                final_remainder = final_remainder[1..].trim();
            }
            return final_remainder.to_string();
        }
    }

    trimmed.to_string()
}

fn normalize_references_section(raw: &str, reference_style: &ReferenceStyle) -> String {
    let cleaned = strip_code_fences(raw).trim().to_string();
    let heading = format!("## {}", reference_style.bibliography_heading());
    if cleaned.starts_with(&heading) {
        cleaned
    } else {
        format!("{heading}\n\n{}", cleaned)
    }
}

fn strip_code_fences(text: &str) -> String {
    text.replace("```markdown", "")
        .replace("```json", "")
        .replace("```", "")
        .trim()
        .to_string()
}

fn evaluate_section_quality(
    section: &str,
    blueprint: &SectionBlueprint,
) -> SectionQualityAssessment {
    SectionQualityAssessment {
        heading_present: section.contains(&format!("## {}", blueprint.heading)),
        word_count: count_words(section),
        minimum_word_count: minimum_words_for_section(blueprint),
        citation_mentions: count_citation_mentions(section),
        minimum_citations: blueprint.minimum_citations,
        bullet_lines: count_bullet_lines(section),
        banned_phrase_hits: collect_banned_phrase_hits(section),
    }
}

fn evaluate_section_part_quality(
    fragment: &str,
    blueprint: &SectionPartBlueprint,
) -> SectionPartQualityAssessment {
    SectionPartQualityAssessment {
        word_count: count_words(fragment),
        minimum_word_count: minimum_words_for_section_part(blueprint),
        citation_mentions: count_citation_mentions(fragment),
        minimum_citations: blueprint.minimum_citations,
        bullet_lines: count_bullet_lines(fragment),
        banned_phrase_hits: collect_banned_phrase_hits(fragment),
    }
}

fn minimum_words_for_section(blueprint: &SectionBlueprint) -> usize {
    (blueprint.target_words * SECTION_WORD_FLOOR_PERCENT) / 100
}

fn section_token_budget(blueprint: &SectionBlueprint) -> u32 {
    ((blueprint.target_words.saturating_mul(2)).min(8_192)) as u32
}

fn minimum_words_for_section_part(blueprint: &SectionPartBlueprint) -> usize {
    (blueprint.target_words * SECTION_PART_WORD_FLOOR_PERCENT) / 100
}

fn section_part_token_budget(blueprint: &SectionPartBlueprint) -> u32 {
    (blueprint.target_words.saturating_mul(2).clamp(768, 4_096)) as u32
}

fn count_bullet_lines(text: &str) -> usize {
    text.lines().filter(|line| is_bullet_line(line)).count()
}

fn collect_banned_phrase_hits(text: &str) -> Vec<String> {
    let mut hits = BTreeSet::new();

    for line in text.lines() {
        let trimmed = line.trim().to_lowercase();
        for marker in BANNED_META_MARKERS {
            if trimmed.starts_with(marker) {
                hits.insert(marker.to_string());
            }
        }

        if trimmed.starts_with("paragraph ")
            || trimmed.starts_with("theme ")
            || trimmed.starts_with("fragment ")
            || trimmed.starts_with("source ")
            || trimmed.starts_with("drafting p")
            || trimmed.starts_with("intro:")
            || trimmed.starts_with("conclusion:")
        {
            hits.insert(
                trimmed
                    .split_whitespace()
                    .take(2)
                    .collect::<Vec<_>>()
                    .join(" "),
            );
        }
    }

    hits.into_iter().collect()
}

fn report_quality_passes(quality: &ResearchQualityReport) -> bool {
    quality.required_headings_present
        && quality.references_present
        && quality.prose_only
        && quality.minimum_word_count_met
        && quality.citation_target_met
}

fn report_failure_summary(quality: &ResearchQualityReport) -> String {
    let mut reasons = Vec::new();
    if !quality.required_headings_present {
        reasons.push(format!(
            "missing headings: {}",
            quality.missing_headings.join(", ")
        ));
    }
    if !quality.references_present {
        reasons.push("missing bibliography/references section".to_string());
    }
    if !quality.bullet_free {
        reasons.push("contains bullet or outline formatting".to_string());
    }
    if !quality.prose_only && !quality.banned_phrase_hits.is_empty() {
        reasons.push(format!(
            "contains prose violations: {}",
            quality.banned_phrase_hits.join(", ")
        ));
    }
    if !quality.minimum_word_count_met {
        reasons.push(format!(
            "word count too low ({} of {})",
            quality.actual_word_count, quality.minimum_word_count
        ));
    }
    if !quality.citation_target_met {
        reasons.push(format!(
            "citation count too low ({} of target {})",
            quality.citation_mentions, quality.citation_target
        ));
    }
    reasons.join("; ")
}

fn identify_sections_for_revision(
    sections: &[ResearchSection],
    spec: &DocumentSpec,
    quality: &ResearchQualityReport,
) -> Vec<usize> {
    let mut indices = BTreeSet::new();

    for (index, blueprint) in spec.section_blueprints.iter().enumerate() {
        if let Some(section) = sections.get(index) {
            let assessment = evaluate_section_quality(&section.content, blueprint);
            if !assessment.passes() {
                indices.insert(index);
            }
        } else {
            indices.insert(index);
        }
    }

    if !quality.minimum_word_count_met || !quality.citation_target_met {
        for (index, section) in sections.iter().enumerate() {
            if section.actual_words < spec.section_blueprints[index].target_words
                || evaluate_section_quality(&section.content, &spec.section_blueprints[index])
                    .citation_mentions
                    < spec.section_blueprints[index].minimum_citations
            {
                indices.insert(index);
            }
        }
    }

    if !quality.prose_only {
        for (index, section) in sections.iter().enumerate() {
            if !collect_banned_phrase_hits(&section.content).is_empty()
                || count_bullet_lines(&section.content) > 0
            {
                indices.insert(index);
            }
        }
    }

    if indices.is_empty() {
        indices.extend(0..sections.len());
    }

    indices.into_iter().collect()
}

fn summarize_section_revision_need(
    section: &ResearchSection,
    blueprint: &SectionBlueprint,
    quality: &ResearchQualityReport,
) -> String {
    let assessment = evaluate_section_quality(&section.content, blueprint);
    let mut reasons = Vec::new();
    let section_failure = assessment.failure_summary();
    if !section_failure.is_empty() {
        reasons.push(section_failure);
    }
    if !quality.minimum_word_count_met {
        reasons.push(
            "the overall paper is still too short and this section must be expanded substantially"
                .to_string(),
        );
    }
    if !quality.citation_target_met {
        reasons.push("the overall paper needs more in-text citations, so this section should cite evidence more often".to_string());
    }
    if !quality.prose_only {
        reasons.push("the overall paper still contains outline/meta language and this section must read as clean final prose".to_string());
    }
    reasons.join("; ")
}

fn estimate_page_count(word_count: usize) -> usize {
    let adjusted = word_count.max(1);
    adjusted.div_ceil(WORDS_PER_PDF_PAGE_ESTIMATE)
}

fn count_words(text: &str) -> usize {
    text.split_whitespace().count()
}

fn is_bullet_line(line: &str) -> bool {
    let trimmed = line.trim_start();
    trimmed.starts_with("- ")
        || trimmed.starts_with("* ")
        || trimmed.starts_with("• ")
        || is_numbered_list(trimmed)
}

fn is_numbered_list(line: &str) -> bool {
    let mut chars = line.chars().peekable();
    let mut saw_digit = false;

    while let Some(ch) = chars.peek() {
        if ch.is_ascii_digit() {
            saw_digit = true;
            chars.next();
        } else {
            break;
        }
    }

    saw_digit && matches!(chars.next(), Some('.')) && matches!(chars.next(), Some(' '))
}

fn count_citation_mentions(text: &str) -> usize {
    let mut count = 0;
    let mut in_parens = false;
    let mut buffer = String::new();

    for ch in text.chars() {
        match ch {
            '(' => {
                in_parens = true;
                buffer.clear();
            }
            ')' if in_parens => {
                let has_letters = buffer.chars().any(|ch| ch.is_alphabetic());
                let has_digits = buffer.chars().any(|ch| ch.is_ascii_digit());
                if has_letters && (has_digits || buffer.contains(',')) {
                    count += 1;
                }
                in_parens = false;
            }
            _ if in_parens => buffer.push(ch),
            _ => {}
        }
    }

    count
}

fn find_year_hint(text: &str) -> Option<i32> {
    text.split(|ch: char| !ch.is_ascii_digit())
        .filter(|token| token.len() == 4)
        .filter_map(|token| token.parse::<i32>().ok())
        .find(|year| (1900..=2100).contains(year))
}

fn domain_label(url: &str) -> String {
    let without_scheme = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))
        .unwrap_or(url);
    without_scheme
        .split('/')
        .next()
        .unwrap_or("web source")
        .trim_start_matches("www.")
        .to_string()
}

fn slugify(text: &str) -> String {
    let slug = text
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();

    slug.split('-')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("-")
}

fn extract_json_object(text: &str) -> Result<String, String> {
    let start = text
        .find('{')
        .ok_or_else(|| "No JSON object start found in planner response".to_string())?;
    let end = text
        .rfind('}')
        .ok_or_else(|| "No JSON object end found in planner response".to_string())?;

    Ok(text[start..=end].to_string())
}

fn should_retry_model_status(status: reqwest::StatusCode, _err_msg: &str) -> bool {
    matches!(status.as_u16(), 429 | 503)
}

/// Extract a retry delay hint from a Google API error message like
/// "Please retry in 14.59s".
fn extract_retry_delay_secs(err_msg: &str) -> Option<u64> {
    let lower = err_msg.to_lowercase();
    if let Some(pos) = lower.find("retry in ") {
        let after = &lower[pos + 9..];
        let num_str: String = after.chars().take_while(|c| c.is_ascii_digit() || *c == '.').collect();
        num_str.parse::<f64>().ok().map(|secs| (secs.ceil() as u64).max(5))
    } else {
        None
    }
}

fn fallback_references(sources: &[ResearchSource], reference_style: &ReferenceStyle) -> String {
    let mut references = format!("## {}\n\n", reference_style.bibliography_heading());

    if sources.is_empty() {
        references.push_str("Retrieved live sources were unavailable, so no source-specific references could be compiled.");
        return references;
    }

    for source in sources {
        references.push_str(&source.citation_hint);
        references.push('\n');
    }

    references.trim_end().to_string()
}

fn section_body_preview(section_markdown: &str, max_len: usize) -> String {
    let body = section_markdown
        .lines()
        .skip_while(|line| line.trim().starts_with("## "))
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_string();
    truncate(&body, max_len)
}

// ── Telegram Inline Keyboard ──────────────────────────────────────

/// Send a message with inline keyboard buttons to a Telegram user.
pub async fn send_telegram_inline_keyboard(
    token: &str,
    chat_id: i64,
    text: &str,
    buttons: Vec<Vec<(String, String)>>,
) -> Result<(), reqwest::Error> {
    let url = format!("https://api.telegram.org/bot{token}/sendMessage");

    let inline_keyboard: Vec<Vec<serde_json::Value>> = buttons
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|(label, callback_data)| {
                    serde_json::json!({
                        "text": label,
                        "callback_data": callback_data
                    })
                })
                .collect()
        })
        .collect();

    let body = serde_json::json!({
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown",
        "reply_markup": {
            "inline_keyboard": inline_keyboard
        }
    });

    reqwest::Client::new().post(&url).json(&body).send().await?;

    Ok(())
}

/// Acknowledge a Telegram callback query (removes the "loading" spinner).
pub async fn answer_callback_query(token: &str, callback_query_id: &str) -> Result<(), String> {
    let url = format!("https://api.telegram.org/bot{token}/answerCallbackQuery");

    let body = serde_json::json!({
        "callback_query_id": callback_query_id
    });

    reqwest::Client::new()
        .post(&url)
        .json(&body)
        .send()
        .await
        .map_err(|error| format!("Failed to answer callback query: {error}"))?;

    Ok(())
}

/// Send the post-report feedback keyboard.
pub async fn send_feedback_keyboard(token: &str, chat_id: i64) {
    let buttons = vec![
        vec![
            ("⭐ Excellent".to_string(), "rate_excellent".to_string()),
            ("👍 Good".to_string(), "rate_good".to_string()),
        ],
        vec![
            ("🔧 Needs Work".to_string(), "rate_needs_work".to_string()),
            ("👎 Poor".to_string(), "rate_poor".to_string()),
        ],
    ];

    let _ = send_telegram_inline_keyboard(
        token,
        chat_id,
        "📊 *How was this report?*\n\nYour feedback helps Professor AI improve\\. Please rate the quality:",
        buttons,
    )
    .await;
}

/// Send the optional comment prompt after a rating.
pub async fn send_comment_prompt(token: &str, chat_id: i64, rating: &FeedbackRating) {
    let buttons = vec![vec![(
        "⏭ Skip Comment".to_string(),
        "skip_comment".to_string(),
    )]];

    let _ = send_telegram_inline_keyboard(
        token,
        chat_id,
        &format!(
            "Thank you for rating: {}\\!\n\n\
             💬 Want to add specific feedback? Just type it below, \
             or tap *Skip* to finish\\.",
            rating.label()
        ),
        buttons,
    )
    .await;
}

// ── Telegram Long-Polling ─────────────────────────────────────────

/// Start Telegram long-polling loop (used when no WEBHOOK_URL is set).
pub async fn start_telegram_polling(state: std::sync::Arc<AppState>) {
    tracing::info!("📡 Starting Telegram long-polling mode...");

    // Delete any existing webhook so getUpdates works
    let delete_url = format!(
        "https://api.telegram.org/bot{}/deleteWebhook",
        state.config.telegram_bot_token
    );
    let _ = reqwest::Client::new()
        .post(&delete_url)
        .json(&serde_json::json!({}))
        .send()
        .await;

    let mut offset: Option<u64> = None;
    let client = reqwest::Client::new();

    loop {
        let url = format!(
            "https://api.telegram.org/bot{}/getUpdates",
            state.config.telegram_bot_token
        );

        let mut params = serde_json::json!({
            "timeout": 30,
            "allowed_updates": ["message", "callback_query"]
        });

        if let Some(off) = offset {
            params["offset"] = serde_json::json!(off);
        }

        let response = match client.post(&url).json(&params).send().await {
            Ok(response) => response,
            Err(error) => {
                tracing::warn!("Polling error: {error}");
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                continue;
            }
        };

        let json: serde_json::Value = match response.json().await {
            Ok(json) => json,
            Err(error) => {
                tracing::warn!("Polling parse error: {error}");
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                continue;
            }
        };

        if let Some(updates) = json["result"].as_array() {
            for update_value in updates {
                // Track offset
                if let Some(update_id) = update_value["update_id"].as_u64() {
                    offset = Some(update_id + 1);
                }

                // Parse and dispatch
                match serde_json::from_value::<TelegramUpdate>(update_value.clone()) {
                    Ok(update) => {
                        let state_clone = state.clone();
                        tokio::spawn(async move {
                            crate::routes::dispatch_telegram_update(&state_clone, update).await;
                        });
                    }
                    Err(error) => {
                        tracing::warn!("Failed to parse update: {error}");
                    }
                }
            }
        }
    }
}

/// Register a Telegram webhook URL.
pub async fn set_telegram_webhook(token: &str, webhook_url: &str) -> Result<(), String> {
    let url = format!("https://api.telegram.org/bot{token}/setWebhook");

    let body = serde_json::json!({
        "url": webhook_url,
        "allowed_updates": ["message", "callback_query"]
    });

    let response = reqwest::Client::new()
        .post(&url)
        .json(&body)
        .send()
        .await
        .map_err(|error| format!("Failed to set webhook: {error}"))?;

    let json: serde_json::Value = response
        .json()
        .await
        .map_err(|error| format!("Failed to parse webhook response: {error}"))?;

    if json["ok"].as_bool() == Some(true) {
        tracing::info!("✅ Telegram webhook set to: {webhook_url}");
        Ok(())
    } else {
        Err(format!(
            "Telegram setWebhook failed: {}",
            json["description"].as_str().unwrap_or("unknown error")
        ))
    }
}

// ── Feedback Persistence ──────────────────────────────────────────

/// Save a feedback entry as a JSON file.
pub async fn save_feedback(feedback_dir: &str, feedback: &UserFeedback) -> Result<(), String> {
    fs::create_dir_all(feedback_dir)
        .await
        .map_err(|error| format!("Failed to create feedback dir: {error}"))?;

    let filename = format!(
        "{}/{}-{}.json",
        feedback_dir,
        feedback.timestamp.format("%Y%m%dT%H%M%SZ"),
        feedback.chat_id
    );

    let json = serde_json::to_string_pretty(feedback)
        .map_err(|error| format!("Failed to serialize feedback: {error}"))?;

    fs::write(&filename, json)
        .await
        .map_err(|error| format!("Failed to write feedback file: {error}"))?;

    tracing::info!("💬 Feedback saved to {filename}");
    Ok(())
}

/// Load recent feedback entries from disk (async version).
pub async fn load_recent_feedback(feedback_dir: &str, limit: usize) -> Vec<UserFeedback> {
    load_recent_feedback_sync(feedback_dir, limit)
}

/// Load recent feedback entries from disk (sync version for startup).
fn load_recent_feedback_sync(feedback_dir: &str, limit: usize) -> Vec<UserFeedback> {
    let dir = std::path::Path::new(feedback_dir);
    if !dir.exists() {
        return Vec::new();
    }

    let mut entries: Vec<_> = match std::fs::read_dir(dir) {
        Ok(entries) => entries
            .filter_map(|entry| entry.ok())
            .filter(|entry| {
                entry
                    .path()
                    .extension()
                    .map(|ext| ext == "json")
                    .unwrap_or(false)
            })
            .collect(),
        Err(_) => return Vec::new(),
    };

    // Sort by filename (descending = most recent first, since filenames start with timestamps)
    entries.sort_by_key(|b| std::cmp::Reverse(b.file_name()));
    entries.truncate(limit);

    entries
        .into_iter()
        .filter_map(|entry| {
            let content = std::fs::read_to_string(entry.path()).ok()?;
            serde_json::from_str::<UserFeedback>(&content).ok()
        })
        .collect()
}

/// Load all feedback entries (for admin endpoint).
pub async fn load_all_feedback(feedback_dir: &str) -> Vec<UserFeedback> {
    load_recent_feedback(feedback_dir, 1000).await
}

// ── Feedback-Aware Prompt Augmentation ─────────────────────────────

/// Build an augmented system prompt that includes patterns from user feedback.
pub fn build_feedback_aware_prompt(base_prompt: &str, feedback: &[UserFeedback]) -> String {
    if feedback.is_empty() {
        return base_prompt.to_string();
    }

    // Aggregate feedback patterns
    let total = feedback.len();
    let excellent = feedback
        .iter()
        .filter(|f| f.rating == FeedbackRating::Excellent)
        .count();
    let good = feedback
        .iter()
        .filter(|f| f.rating == FeedbackRating::Good)
        .count();
    let needs_work = feedback
        .iter()
        .filter(|f| f.rating == FeedbackRating::NeedsWork)
        .count();
    let poor = feedback
        .iter()
        .filter(|f| f.rating == FeedbackRating::Poor)
        .count();

    // Collect recent comments (last 10 with comments)
    let recent_comments: Vec<String> = feedback
        .iter()
        .filter_map(|f| {
            f.comment
                .as_ref()
                .map(|c| format!("- [{}] {}", f.rating.label(), c))
        })
        .take(10)
        .collect();

    let mut augmented = base_prompt.to_string();
    augmented.push_str("\n\n--- USER FEEDBACK INTELLIGENCE ---\n");
    augmented.push_str(&format!(
        "Based on {} user evaluations: {} excellent, {} good, {} needs work, {} poor.\n",
        total, excellent, good, needs_work, poor
    ));

    if !recent_comments.is_empty() {
        augmented.push_str("\nRecent user comments on generated reports:\n");
        for comment in &recent_comments {
            augmented.push_str(comment);
            augmented.push('\n');
        }
    }

    if needs_work + poor > total / 3 {
        augmented.push_str(
            "\nIMPORTANT: A significant portion of users found reports lacking. \
             Focus on depth, accuracy, proper citations, and avoiding generic filler.\n",
        );
    }

    augmented.push_str("Apply these insights to improve the quality of all future reports.\n");
    augmented.push_str("--- END FEEDBACK INTELLIGENCE ---\n");

    augmented
}

// ── Rate Limiting ─────────────────────────────────────────────────

/// Check if a chat has exceeded the rate limit. Returns true if the request is allowed.
pub fn check_rate_limit(state: &AppState, chat_id: i64) -> bool {
    let max_requests = state.config.max_requests_per_hour;
    let mut rate_limits = state.rate_limits.lock().unwrap();
    let now = std::time::Instant::now();
    let one_hour = std::time::Duration::from_secs(3600);

    let timestamps = rate_limits.entry(chat_id).or_default();

    // Remove expired entries
    timestamps.retain(|ts| now.duration_since(*ts) < one_hour);

    if timestamps.len() >= max_requests {
        return false;
    }

    timestamps.push(now);
    true
}

// ── Input Sanitization ────────────────────────────────────────────

/// Sanitize user topic input: trim, limit length, strip control characters.
pub fn sanitize_topic(text: &str) -> String {
    text.trim()
        .chars()
        .filter(|c| !c.is_control() || *c == '\n')
        .take(MAX_TOPIC_LENGTH)
        .collect::<String>()
        .trim()
        .to_string()
}

// ── Session Helpers ───────────────────────────────────────────────

/// Get the current conversation state for a chat.
pub fn get_session(state: &AppState, chat_id: i64) -> ConversationState {
    state
        .sessions
        .lock()
        .unwrap()
        .get(&chat_id)
        .cloned()
        .unwrap_or(ConversationState::Idle)
}

/// Set the conversation state for a chat.
pub fn set_session(state: &AppState, chat_id: i64, new_state: ConversationState) {
    state.sessions.lock().unwrap().insert(chat_id, new_state);
}

/// Add feedback to the in-memory cache.
pub fn cache_feedback(state: &AppState, feedback: UserFeedback) {
    let mut cache = state.feedback_cache.lock().unwrap();
    cache.insert(0, feedback);
    cache.truncate(100); // Keep last 100
}

/// Get a snapshot of the current feedback cache.
pub fn get_feedback_snapshot(state: &AppState) -> Vec<UserFeedback> {
    state.feedback_cache.lock().unwrap().clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_json_handles_fenced_or_prefixed_text() {
        let raw = "Here you go\n```json\n{\"title\":\"Test\",\"thesis\":\"A\",\"methodology\":\"B\",\"research_questions\":[\"Q1\"],\"keywords\":[\"k\"],\"writing_guidance\":\"Guide\"}\n```";
        let json = extract_json_object(raw).expect("json object");
        assert!(json.starts_with('{'));
        assert!(json.ends_with('}'));
    }

    #[test]
    fn quality_flags_missing_headings_and_bullets() {
        let spec = document_spec_for_tier(&ResearchTier::Preview);
        let markdown = "# Title\n\n## Executive Summary\n\nGood prose.\n\n- bullet\n";
        let quality = analyze_report_quality(markdown, &spec, 3, &ReferenceStyle::Apa);
        assert!(!quality.required_headings_present);
        assert!(!quality.bullet_free);
        assert!(quality
            .missing_headings
            .contains(&"Background and Context".to_string()));
    }

    #[test]
    fn banned_markers_are_detected() {
        let hits = collect_banned_phrase_hits("Drafting:\nParagraph 1:\nReal prose.");
        assert!(hits.iter().any(|hit| hit.contains("drafting:")));
        assert!(hits.iter().any(|hit| hit.contains("paragraph")));
    }

    #[test]
    fn quality_ignores_numbered_references_for_prose_and_citations() {
        let spec = DocumentSpec {
            deliverable: "paper",
            minimum_words: 10,
            section_blueprints: vec![SectionBlueprint {
                heading: "Introduction",
                brief: "Set up the topic",
                target_words: 10,
                citation_expectation: "Use at least one citation.",
                minimum_citations: 1,
                parts: Vec::new(),
            }],
            include_source_matrix: false,
        };
        let markdown = "# Title\n\n## Introduction\n\nFinal prose with evidence (Ada, 2024).\n\n## References\n\n1. Ada, A. (2024). Example article.\n2. Baker, B. (2023). Another article.\n";
        let quality = analyze_report_quality(markdown, &spec, 1, &ReferenceStyle::Apa);

        assert!(quality.references_present);
        assert!(quality.bullet_free);
        assert!(quality.prose_only);
        assert_eq!(quality.citation_mentions, 1);
        assert_eq!(
            quality.actual_word_count,
            count_words(report_body_for_quality(markdown, &ReferenceStyle::Apa))
        );
    }

    #[test]
    fn quota_exhaustion_429_is_not_retried() {
        assert!(!should_retry_model_status(
            reqwest::StatusCode::TOO_MANY_REQUESTS,
            "Quota exceeded for metric: generativelanguage.googleapis.com/generate_content_paid_tier_input_token_count"
        ));
        assert!(should_retry_model_status(
            reqwest::StatusCode::TOO_MANY_REQUESTS,
            "Transient rate limit hit, please retry in 10 seconds"
        ));
        assert!(should_retry_model_status(
            reqwest::StatusCode::SERVICE_UNAVAILABLE,
            "Backend overloaded"
        ));
    }

    #[test]
    fn slugify_collapses_symbols() {
        assert_eq!(
            slugify("AI & Supply Chain: Nigeria 2025"),
            "ai-supply-chain-nigeria-2025"
        );
    }

    #[test]
    fn normalize_section_prepends_heading() {
        let normalized = normalize_section_markdown("This is the body.", "Introduction");
        assert!(normalized.starts_with("## Introduction"));
        assert!(normalized.contains("This is the body."));
    }

    #[test]
    fn normalize_section_part_strips_accidental_headings() {
        let normalized = normalize_section_part("### Subheading\n\nThis is the actual prose.");
        assert_eq!(normalized, "This is the actual prose.");
    }

    #[test]
    fn convert_outline_fragment_to_prose_flattens_bullets() {
        let converted = convert_outline_fragment_to_prose(
            "- Context: AI improves forecasting\n- Benefit: Firms react faster to demand shifts",
        );
        assert!(!converted.contains("- "));
        assert!(converted.contains("AI improves forecasting."));
        assert!(converted.contains("Firms react faster to demand shifts."));
    }

    #[test]
    fn convert_outline_section_to_prose_preserves_main_heading() {
        let converted = convert_outline_section_to_prose(
            "## Introduction\n\n- Context: AI improves forecasting",
            "Introduction",
        );
        assert!(converted.starts_with("## Introduction"));
        assert!(converted.contains("AI improves forecasting."));
    }

    #[test]
    fn assemble_section_from_parts_wraps_fragments_under_main_heading() {
        let assembled = assemble_section_from_parts(
            "Introduction",
            &[
                "First paragraph.".to_string(),
                "Second paragraph.".to_string(),
            ],
        );
        assert!(assembled.starts_with("## Introduction"));
        assert!(assembled.contains("First paragraph."));
        assert!(assembled.contains("Second paragraph."));
    }

    #[test]
    fn page_estimate_rounds_up() {
        assert_eq!(estimate_page_count(1), 1);
        assert_eq!(estimate_page_count(500), 1);
        assert_eq!(estimate_page_count(501), 2);
    }
}
pub async fn send_whatsapp_message(to: &str, text: &str) {
    let payload = serde_json::json!({
        "to": to,
        "text": text
    });
    match reqwest::Client::new()
        .post("http://localhost:8002/send")
        .header(
            "X-Bridge-Auth",
            std::env::var("BRIDGE_SECRET").unwrap_or_else(|_| "local_dev_secret_123".to_string()),
        )
        .json(&payload)
        .send()
        .await
    {
        Ok(res) => {
            if !res.status().is_success() {
                tracing::error!("Failed to push to Node bridge: {}", res.status());
            }
        }
        Err(e) => {
            tracing::error!("Error pushing to Node bridge: {}", e);
        }
    }
}

pub async fn send_whatsapp_document(to: &str, text: &str, db64: &str, fname: &str, cap: &str) {
    let payload = serde_json::json!({
        "to": to,
        "text": text,
        "document_base64": db64,
        "document_filename": fname,
        "document_caption": cap
    });
    match reqwest::Client::new()
        .post("http://localhost:8002/send")
        .header(
            "X-Bridge-Auth",
            std::env::var("BRIDGE_SECRET").unwrap_or_else(|_| "local_dev_secret_123".to_string()),
        )
        .json(&payload)
        .send()
        .await
    {
        Ok(res) => {
            if !res.status().is_success() {
                tracing::error!("Failed to push doc to Node bridge: {}", res.status());
            }
        }
        Err(e) => {
            tracing::error!("Error pushing doc to Node bridge: {}", e);
        }
    }
}
