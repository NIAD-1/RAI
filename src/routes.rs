//! HTTP route handlers for Telegram, Paystack, health, admin, and simulation.

use axum::{
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use chrono::Utc;
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    sync::Arc,
};
use uuid::Uuid;

use crate::models::*;
use crate::services::{self, AppState};

// ── Health Check ───────────────────────────────────────────────────

pub async fn health_check() -> impl IntoResponse {
    Json(serde_json::json!({
        "status": "ok",
        "service": "research-bot",
        "version": "0.2.0"
    }))
}

// ── WhatsApp Bridge Handlers ──────────────────────────────────────

pub async fn whatsapp_bridge_incoming(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(payload): Json<WhatsAppBridgeMessage>,
) -> impl IntoResponse {
    let auth_header = headers.get("X-Bridge-Auth").and_then(|v| v.to_str().ok()).unwrap_or("");
    if auth_header != state.config.bridge_secret {
        return (StatusCode::UNAUTHORIZED, Json(WhatsAppBridgeResponse {
            reply: Some("Unauthorized access to API.".to_string()),
            document_base64: None,
            document_filename: None,
            document_caption: None,
        })).into_response();
    }

    let sender_key = whatsapp_sender_key(&payload.sender);

    let text = services::sanitize_topic(&payload.body);
    if text.is_empty() {
        return Json(WhatsAppBridgeResponse {
            reply: Some(whatsapp_help_text().to_string()),
            document_base64: None,
            document_filename: None,
            document_caption: None,
        }).into_response();
    }

    let lower = text.to_lowercase();
    let is_greeting = match lower.as_str() {
        "help" | "/help" | "start" | "/start" | "menu" | "hi" | "hello" | "hey" | "hi professor" | "hello professor" => true,
        _ => lower.len() < 15 && (lower.starts_with("hi ") || lower.starts_with("hello ") || lower.starts_with("hey ")),
    };
    if is_greeting {
        return Json(WhatsAppBridgeResponse {
            reply: Some(whatsapp_help_text().to_string()),
            document_base64: None,
            document_filename: None,
            document_caption: None,
        }).into_response();
    }

    // Handle /clear command to reset conversation memory
    if lower == "/clear" || lower == "clear" || lower == "reset" || lower == "/reset" {
        if let Ok(mut history) = state.chat_history.lock() {
            history.remove(&sender_key);
        }
        return Json(WhatsAppBridgeResponse {
            reply: Some("🔄 Your conversation memory has been cleared! I'll treat your next message as a fresh topic.".to_string()),
            document_base64: None,
            document_filename: None,
            document_caption: None,
        }).into_response();
    }


    if !services::check_rate_limit(&state, sender_key) {
        return Json(WhatsAppBridgeResponse {
            reply: Some(RATE_LIMIT_MESSAGE.to_string()),
            document_base64: None,
            document_filename: None,
            document_caption: None,
        }).into_response();
    }

    {
        let mut active_tasks = state.active_tasks.lock().unwrap();
        if active_tasks.contains(&sender_key) {
            return Json(WhatsAppBridgeResponse {
                reply: Some("⏳ You already have an active research task running. Please wait for it to complete before starting another one.".to_string()),
                document_base64: None,
                document_filename: None,
                document_caption: None,
            }).into_response();
        }
        active_tasks.insert(sender_key);
    }

    let phone = payload.sender.clone();
    let text_clone = text.clone();
    let state_clone = state.clone();

    // Spawn async wrapper
    tokio::spawn(async move {
        // ── Conversational memory: reformulate follow-ups ──────────
        let history = services::get_chat_history(&state_clone, sender_key);
        let resolved_text = services::reformulate_query(
            &state_clone.config.google_api_key,
            &state_clone.config.ai_model,
            &history,
            &text_clone,
        ).await;

        if let Some(report_topic) = extract_full_report_topic(&resolved_text) {
            // ── Global report queue: limit concurrent reports ──────────
            let sem = state_clone.report_semaphore.clone();
            let available = sem.available_permits();
            if available == 0 {
                services::send_whatsapp_message(&phone, "📋 Your report is queued! Other reports are being generated right now. I'll start yours automatically when a slot opens...").await;
            }
            let _permit = match sem.acquire().await {
                Ok(p) => p,
                Err(_) => {
                    services::send_whatsapp_message(&phone, &sanitize_error_for_user("semaphore closed", &report_topic)).await;
                    if let Ok(mut active_tasks) = state_clone.active_tasks.lock() {
                        active_tasks.remove(&sender_key);
                    }
                    return;
                }
            };

            let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(100);
            let rx_phone = phone.clone();
            tokio::spawn(async move {
                while let Some(msg) = rx.recv().await {
                    services::send_whatsapp_message(&rx_phone, &msg).await;
                }
            });

            services::send_whatsapp_message(&phone, &format!("⏳ Preparing full research report for \"{report_topic}\"...")).await;

            let (topic, reference_style) = extract_topic_and_reference_style(&report_topic);
            let tier = ResearchTier::Complete;
            
            match services::run_research_agent(
                &state_clone.config.google_api_key,
                &state_clone.config.youtube_api_key,
                &state_clone.config.tavily_api_key,
                &state_clone.config.ai_model,
                &topic,
                &tier,
                &reference_style,
                Some(tx),
            ).await {
                Ok(report) => {
                    let pdf = report.artifacts.as_ref().and_then(|a| a.pdf_path.as_ref()).and_then(|p| std::fs::read(p).ok());
                    if let Some(pdf_bytes) = pdf {
                        let b64 = base64::engine::general_purpose::STANDARD.encode(&pdf_bytes);
                        services::send_whatsapp_document(&phone, "", &b64, "report.pdf", &format!("📊 Your Research Report: {topic}")).await;
                    } else {
                        services::send_whatsapp_message(&phone, &format!("✅ Research completed for \"{topic}\" but failed to attach PDF.")).await;
                    }
                },
                Err(error) => {
                    services::send_whatsapp_message(&phone, &sanitize_error_for_user(&error, &topic)).await;
                }
            }

            // Record the resolved topic in conversation history
            services::push_chat_history(&state_clone, sender_key, &report_topic);
        } else {
            let (topic, _) = extract_topic_and_reference_style(&resolved_text);
            services::send_whatsapp_message(&phone, &format!("🔎 Researching brief for \"{topic}\"...")).await;

            match services::run_whatsapp_research_brief(
                &state_clone.config.google_api_key,
                &state_clone.config.youtube_api_key,
                &state_clone.config.tavily_api_key,
                &state_clone.config.ai_model,
                &topic,
            ).await {
                Ok(brief) => {
                    services::send_whatsapp_message(&phone, &brief.answer).await;
                },
                Err(error) => {
                    services::send_whatsapp_message(&phone, &sanitize_error_for_user(&error, &topic)).await;
                }
            }

            // Record the resolved topic in conversation history
            services::push_chat_history(&state_clone, sender_key, &topic);
        }

        // Clean up the active task lock for this user so they can initiate another search
        if let Ok(mut active_tasks) = state_clone.active_tasks.lock() {
            active_tasks.remove(&sender_key);
        }
    });

    Json(WhatsAppBridgeResponse {
        reply: None,
        document_base64: None,
        document_filename: None,
        document_caption: None,
    }).into_response()
}

pub async fn whatsapp_bridge_audio(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(payload): Json<WhatsAppBridgeAudio>,
) -> impl IntoResponse {
    let auth_header = headers.get("X-Bridge-Auth").and_then(|v| v.to_str().ok()).unwrap_or("");
    if auth_header != state.config.bridge_secret {
        return (StatusCode::UNAUTHORIZED, Json(WhatsAppBridgeResponse {
            reply: Some("Unauthorized access to API.".to_string()),
            document_base64: None,
            document_filename: None,
            document_caption: None,
        })).into_response();
    }

    let sender_key = whatsapp_sender_key(&payload.sender);
    Json(WhatsAppBridgeResponse {
        reply: Some(
            "Voice note research is not enabled yet. Send text like `Explain the future of lithium batteries` or `report: impact of AI on healthcare logistics`."
                .to_string(),
        ),
        document_base64: None,
        document_filename: None,
        document_caption: None,
    }).into_response()
}

// ── Telegram Webhook Handler ───────────────────────────────────────

pub async fn telegram_webhook(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<TelegramUpdate>,
) -> impl IntoResponse {
    let state_clone = state.clone();
    tokio::spawn(async move {
        dispatch_telegram_update(&state_clone, payload).await;
    });

    StatusCode::OK
}

/// Central dispatcher — handles both messages and callback queries.
/// Called from both the webhook handler and the polling loop.
pub async fn dispatch_telegram_update(state: &AppState, update: TelegramUpdate) {
    // Handle callback queries (inline keyboard button presses)
    if let Some(callback) = update.callback_query {
        handle_callback_query(state, callback).await;
        return;
    }

    // Handle text messages
    if let Some(message) = update.message {
        let chat_id = message.chat.id;
        if let Some(text) = message.text {
            let username = message.from.as_ref().and_then(|u| u.username.clone());
            handle_user_message(state, chat_id, &text, username).await;
        }
    }
}

// ── Callback Query Handler (Inline Keyboard) ──────────────────────

async fn handle_callback_query(state: &AppState, callback: TelegramCallbackQuery) {
    let token = &state.config.telegram_bot_token;
    let chat_id = callback.message.as_ref().map(|m| m.chat.id).unwrap_or(0);
    let callback_data = callback.data.as_deref().unwrap_or("");

    // Acknowledge the button press immediately
    let _ = services::answer_callback_query(token, &callback.id).await;

    // Handle "skip comment" button
    if callback_data == "skip_comment" {
        let session = services::get_session(state, chat_id);
        if let ConversationState::AwaitingComment { topic, rating } = session {
            // Save feedback without comment
            let feedback = UserFeedback {
                chat_id,
                username: callback.from.username.clone(),
                topic: topic.clone(),
                rating,
                comment: None,
                timestamp: Utc::now(),
            };

            if let Err(error) = services::save_feedback(&state.config.feedback_dir, &feedback).await
            {
                tracing::error!("Failed to save feedback: {error}");
            }
            services::cache_feedback(state, feedback);
            services::set_session(state, chat_id, ConversationState::Idle);

            let _ = services::send_telegram_message(
                token,
                chat_id,
                "✅ Thank you for your feedback! It helps Professor AI improve.\n\nSend another topic whenever you're ready.",
            )
            .await;
        }
        return;
    }

    // Handle rating buttons
    if let Some(rating) = FeedbackRating::from_callback_data(callback_data) {
        let session = services::get_session(state, chat_id);
        if let ConversationState::AwaitingFeedback { topic } = session {
            // Transition to awaiting comment
            services::set_session(
                state,
                chat_id,
                ConversationState::AwaitingComment {
                    topic,
                    rating: rating.clone(),
                },
            );

            services::send_comment_prompt(token, chat_id, &rating).await;
        }
    }
}

// ── Message Handler ────────────────────────────────────────────────

async fn handle_user_message(state: &AppState, chat_id: i64, text: &str, username: Option<String>) {
    let token = &state.config.telegram_bot_token;
    let lower = text.trim().to_lowercase();

    // ── Command handling ──────────────────────────────────────────

    if lower == "/start" || lower == "hi" || lower == "hello" || lower == "menu" {
        let _ = services::send_telegram_markdown_message(token, chat_id, GREETING_MESSAGE).await;
        return;
    }

    if lower == "/help" {
        let _ = services::send_telegram_markdown_message(token, chat_id, HELP_MESSAGE).await;
        return;
    }

    if lower == "/terms" {
        let _ = services::send_telegram_markdown_message(token, chat_id, TERMS_MESSAGE).await;
        return;
    }

    if lower == "/status" {
        let session = services::get_session(state, chat_id);
        let status_text = match &session {
            ConversationState::Idle => "📭 No active research. Send me a topic to get started!",
            ConversationState::Researching { topic } => &format!(
                "🔍 Currently researching: \"{topic}\"\n\nPlease wait for the report to complete."
            ),
            ConversationState::AwaitingFeedback { topic } => &format!(
                "📊 Report on \"{topic}\" was delivered. Please rate it using the buttons above!"
            ),
            ConversationState::AwaitingComment { topic, rating } => &format!(
                "💬 You rated the report on \"{topic}\" as {}. Type a comment or tap Skip.",
                rating.label()
            ),
        };
        let _ = services::send_telegram_message(token, chat_id, status_text).await;
        return;
    }

    if lower == "/feedback" {
        let session = services::get_session(state, chat_id);
        match session {
            ConversationState::AwaitingFeedback { .. } => {
                let _ = services::send_telegram_message(
                    token,
                    chat_id,
                    "Please use the rating buttons above to rate your last report.",
                )
                .await;
            }
            _ => {
                let _ = services::send_telegram_message(
                    token,
                    chat_id,
                    "No report pending feedback. Submit a research topic first!",
                )
                .await;
            }
        }
        return;
    }

    // ── State-aware message handling ──────────────────────────────

    let session = services::get_session(state, chat_id);

    // If awaiting a comment, capture it as feedback
    if let ConversationState::AwaitingComment { topic, rating } = session {
        let feedback = UserFeedback {
            chat_id,
            username: username.clone(),
            topic: topic.clone(),
            rating,
            comment: Some(text.trim().to_string()),
            timestamp: Utc::now(),
        };

        if let Err(error) = services::save_feedback(&state.config.feedback_dir, &feedback).await {
            tracing::error!("Failed to save feedback: {error}");
        }
        services::cache_feedback(state, feedback);
        services::set_session(state, chat_id, ConversationState::Idle);

        let _ = services::send_telegram_message(
            token,
            chat_id,
            "✅ Thank you for your detailed feedback! Professor AI will learn from this.\n\nSend another topic whenever you're ready.",
        )
        .await;
        return;
    }

    // If research is in progress, block duplicate requests
    if matches!(session, ConversationState::Researching { .. }) {
        let _ = services::send_telegram_message(token, chat_id, RESEARCH_IN_PROGRESS_MESSAGE).await;
        return;
    }

    // ── Start new research ────────────────────────────────────────

    // Rate limit check
    if !services::check_rate_limit(state, chat_id) {
        let _ = services::send_telegram_message(token, chat_id, RATE_LIMIT_MESSAGE).await;
        return;
    }

    // Sanitize and extract topic
    let sanitized = services::sanitize_topic(text);
    if sanitized.is_empty() {
        let _ =
            services::send_telegram_message(token, chat_id, "Please send a valid research topic.")
                .await;
        return;
    }

    let (topic, reference_style) = extract_topic_and_reference_style(&sanitized);

    // Set state to researching
    services::set_session(
        state,
        chat_id,
        ConversationState::Researching {
            topic: topic.clone(),
        },
    );

    // Determine tier (testing mode = always Complete)
    let tier = if state.config.testing_mode {
        ResearchTier::Complete
    } else {
        ResearchTier::Preview
    };

    let tier_badge = if state.config.testing_mode {
        "🧪 *Beta Testing* — Full report (FREE)"
    } else {
        tier.label()
    };

    let initial_msg = format!(
        "🔍 *Professor is researching:* \"{topic}\"\n\n\
         {tier_badge}\n\
         Reference style: {}\n\n\
         ⏳ *Starting research engine...*",
        reference_style.label()
    );

    let message_id = services::send_telegram_markdown_message(token, chat_id, &initial_msg).await.ok();

    let (progress_tx, mut progress_rx) = tokio::sync::mpsc::channel::<String>(100);
    let token_clone = token.to_string();
    let topic_clone = topic.clone();
    let tier_badge_clone = tier_badge.to_string();
    let ref_style_clone = reference_style.label().to_string();

    if let Some(mid) = message_id {
        tokio::spawn(async move {
            let mut current_status = String::new();
            while let Some(update) = progress_rx.recv().await {
                if !current_status.is_empty() {
                    current_status.push('\n');
                }
                current_status.push_str("— ");
                current_status.push_str(&update);

                let text = format!("🔍 *Professor is researching:* \"{topic_clone}\"\n\n\
                    {tier_badge_clone}\n\
                    Reference style: {}\n\n\
                    {}\n\n\
                    ⏳ *Working...*", ref_style_clone, current_status);
                let _ = services::edit_telegram_message(&token_clone, chat_id, mid, &text).await;
            }
        });
    }

    // Build feedback-aware prompt
    let feedback_snapshot = services::get_feedback_snapshot(state);
    let _augmented_prompt = services::build_feedback_aware_prompt(
        services::PROFESSOR_SYSTEM_PROMPT,
        &feedback_snapshot,
    );

    match services::run_research_agent(
        &state.config.google_api_key,
        &state.config.youtube_api_key,
        &state.config.tavily_api_key,
        &state.config.ai_model,
        &topic,
        &tier,
        &reference_style,
        Some(progress_tx),
    )
    .await
    {
        Ok(report) => {
            let intro = format!(
                "Research complete.\n\nTitle: {}\nDeliverable: {}\nReference style: {}\nWord count: {}\nEstimated pages: {}\nCitation target: {}\n",
                report.title,
                report.deliverable,
                report.reference_style,
                report.total_word_count,
                report.estimated_page_count,
                report.citations_target
            );
            let _ = services::send_telegram_message(token, chat_id, &intro).await;

            if let Some(preview) = &report.abstract_preview {
                let preview_message = format!("Abstract preview:\n\n{}", preview);
                let _ = services::send_telegram_message(token, chat_id, &preview_message).await;
            }

            if !report.sources.is_empty() {
                let mut materials = String::from("📚 Materials Evaluated\n\n");
                for (i, source) in report.sources.iter().enumerate() {
                    let safe_url = if source.url.is_empty() { "" } else { &source.url };
                    materials.push_str(&format!("{}. {}\n{}\n\n", i + 1, source.title, safe_url));
                }
                for chunk in split_message(&materials, 3_800) {
                    let _ = services::send_telegram_message(token, chat_id, &chunk).await;
                }
            }

            let mut closing = String::from(
                "✅ Your draft has been assembled with sections, references, and quality checks.",
            );
            if let Some(artifacts) = &report.artifacts {
                if let Some(path) = &artifacts.markdown_path {
                    closing.push_str(&format!("\nMarkdown saved to: {path}"));
                }
                if let Some(path) = &artifacts.json_path {
                    closing.push_str(&format!("\nJSON package saved to: {path}"));
                }
                if let Some(path) = &artifacts.pdf_path {
                    closing.push_str(&format!("\nPDF saved to: {path}"));
                    let _ = services::send_telegram_document_file(
                        token,
                        chat_id,
                        path,
                        &format!(
                            "*{}*\n{}\n{}\nEstimated pages: {}",
                            report.title,
                            report.deliverable,
                            report.reference_style,
                            report.estimated_page_count
                        ),
                    )
                    .await;
                }
            }
            let _ = services::send_telegram_message(token, chat_id, &closing).await;

            // Transition to feedback state and send rating keyboard
            services::set_session(
                state,
                chat_id,
                ConversationState::AwaitingFeedback {
                    topic: topic.clone(),
                },
            );
            services::send_feedback_keyboard(token, chat_id).await;
        }
        Err(err) => {
            tracing::error!("Research failed for chat {chat_id}: {err}");
            services::set_session(state, chat_id, ConversationState::Idle);
            let _ = services::send_telegram_message(
                token,
                chat_id,
                &format!(
                    "I encountered an issue while preparing the report.\nError: {err}\n\nPlease try again."
                ),
            )
            .await;
        }
    }
}

// ── Paystack Webhook ───────────────────────────────────────────────

pub async fn paystack_webhook(Json(payload): Json<PaystackWebhookPayload>) -> impl IntoResponse {
    tracing::info!("💰 Paystack event: {}", payload.event);

    if payload.event == "charge.success" {
        let amount = payload.data.amount.unwrap_or(0);
        let tier = ResearchTier::from_amount_kobo(amount);
        let phone = payload
            .data
            .metadata
            .as_ref()
            .and_then(|metadata| metadata.get("phone"))
            .and_then(|phone| phone.as_str())
            .unwrap_or("unknown");

        tracing::info!(
            "✅ Payment confirmed: ₦{} for {} from {phone}",
            amount / 100,
            tier.label()
        );
    }

    StatusCode::OK
}

// ── Admin Feedback Endpoint ────────────────────────────────────────

#[derive(serde::Deserialize)]
pub struct AdminQuery {
    pub key: Option<String>,
}

pub async fn admin_feedback(
    State(state): State<Arc<AppState>>,
    Query(query): Query<AdminQuery>,
) -> impl IntoResponse {
    // Simple auth check
    let provided_key = query.key.as_deref().unwrap_or("");
    if provided_key != state.config.admin_api_key {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error": "Invalid admin key"})),
        );
    }

    let feedback = services::load_all_feedback(&state.config.feedback_dir).await;

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

    (
        StatusCode::OK,
        Json(serde_json::json!({
            "total_feedback": total,
            "summary": {
                "excellent": excellent,
                "good": good,
                "needs_work": needs_work,
                "poor": poor,
            },
            "feedback": feedback
        })),
    )
}

// ── Simulation Endpoint (Local Testing) ────────────────────────────

pub async fn simulate_research(
    State(state): State<Arc<AppState>>,
    Json(request): Json<SimulateRequest>,
) -> impl IntoResponse {
    tracing::info!(
        "🧪 Simulation: topic=\"{}\" tier={} reference_style={}",
        request.topic,
        request.tier,
        request.reference_style.as_deref().unwrap_or("default")
    );

    let tier = match request.tier.to_lowercase().as_str() {
        "preview" => ResearchTier::Preview,
        "starter" => ResearchTier::Starter,
        "standard" => ResearchTier::Standard,
        _ => ResearchTier::Complete,
    };
    let reference_style = request
        .reference_style
        .as_deref()
        .and_then(ReferenceStyle::from_user_input)
        .unwrap_or_default();

    match services::run_research_agent(
        &state.config.google_api_key,
        &state.config.youtube_api_key,
        &state.config.tavily_api_key,
        &state.config.ai_model,
        &request.topic,
        &tier,
        &reference_style,
        None,
    )
    .await
    {
        Ok(report) => {
            let job_id = Uuid::new_v4().to_string();
            (
                StatusCode::OK,
                Json(serde_json::json!({
                    "job_id": job_id,
                    "topic": request.topic,
                    "tier": tier.label(),
                    "deliverable": report.deliverable,
                    "reference_style": report.reference_style,
                    "title": report.title,
                    "citations_target": tier.citation_target(),
                    "word_count": report.total_word_count,
                    "estimated_page_count": report.estimated_page_count,
                    "quality": report.quality,
                    "artifacts": report.artifacts,
                    "sources": report.sources,
                    "sections": report.sections,
                    "result": report.markdown,
                    "status": "complete"
                })),
            )
        }
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": err,
                "status": "failed"
            })),
        ),
    }
}

// ── Helpers ────────────────────────────────────────────────────────

fn extract_topic_and_reference_style(text: &str) -> (String, ReferenceStyle) {
    let mut cleaned_lines = Vec::new();
    let mut style = None;

    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let lower = trimmed.to_lowercase();
        if let Some(value) = lower
            .strip_prefix("reference style:")
            .or_else(|| lower.strip_prefix("citation style:"))
            .or_else(|| lower.strip_prefix("style:"))
        {
            style = ReferenceStyle::from_user_input(value.trim());
            continue;
        }

        cleaned_lines.push(trimmed);
    }

    let topic = if cleaned_lines.is_empty() {
        text.trim().to_string()
    } else {
        cleaned_lines.join(" ")
    };

    (topic, style.unwrap_or_default())
}

fn whatsapp_help_text() -> &'static str {
    "👋 Hello! I am Professor AI, an autonomous academic research assistant.\n\nI can investigate topics, synthesize data from YouTube, web searches, public forums, and books, and draft formal reports.\n\n*How to use me:*\n\n1️⃣ *Quick Briefs:*\nJust chat with me! Send any topic (e.g., _\"How does AI affect logistics?\"_) and I'll send back a fast, conversational research summary.\n\n2️⃣ *Full Research Papers:*\nType `report:` followed by your topic. I will spend a few minutes compiling an extensive PDF research paper complete with APA 7th citations and a formal methodology.\n_Example: `report: impact of blockchain on pharmaceutical supply chains`_\n\nSend a topic whenever you're ready!"
}

fn extract_full_report_topic(text: &str) -> Option<String> {
    let trimmed = text.trim();
    let lower = trimmed.to_lowercase();

    for (lower_prefix, original_prefix) in [
        ("report:", "report:"),
        ("full report:", "full report:"),
        ("pdf:", "pdf:"),
        ("/report ", "/report "),
    ] {
        if lower.starts_with(lower_prefix) {
            let original = trimmed[original_prefix.len()..].trim();
            if !original.is_empty() {
                return Some(original.to_string());
            }
        }
    }

    None
}

fn whatsapp_sender_key(sender: &str) -> i64 {
    let digits: String = sender.chars().filter(|c| c.is_ascii_digit()).collect();
    if let Ok(value) = digits.parse::<i64>() {
        return value;
    }

    let mut hasher = DefaultHasher::new();
    sender.hash(&mut hasher);
    (hasher.finish() & (i64::MAX as u64)) as i64
}


fn split_message(text: &str, max_len: usize) -> Vec<String> {
    if text.len() <= max_len {
        return vec![text.to_string()];
    }

    let mut chunks = Vec::new();
    let mut remaining = text;

    while !remaining.is_empty() {
        if remaining.len() <= max_len {
            chunks.push(remaining.to_string());
            break;
        }

        let split_at = remaining[..max_len]
            .rfind('\n')
            .unwrap_or_else(|| remaining[..max_len].rfind(' ').unwrap_or(max_len));

        chunks.push(remaining[..split_at].to_string());
        remaining = remaining[split_at..].trim_start();
    }

    chunks
}

/// Sanitize raw backend error strings into user-friendly WhatsApp messages.
/// Strips URLs, HTTP status codes, and stack traces.
fn sanitize_error_for_user(error: &str, topic: &str) -> String {
    let lower = error.to_lowercase();
    if lower.contains("429") || lower.contains("rate limit") || lower.contains("quota") {
        format!(
            "⏳ I'm temporarily overloaded with requests right now. Please try \"{topic}\" again in about 2 minutes!"
        )
    } else if lower.contains("timeout") || lower.contains("timed out") {
        format!(
            "⏱️ The research for \"{topic}\" took too long. This usually happens with very broad topics. Try being more specific!"
        )
    } else if lower.contains("empty") || lower.contains("no text content") {
        format!(
            "🤔 I couldn't generate a good response for \"{topic}\". Try rephrasing your question!"
        )
    } else {
        format!(
            "❌ I ran into an issue while researching \"{topic}\". Please try again in a moment!"
        )
    }
}

// ── QR Code Page (for WhatsApp linking on Render) ──────────────────

// ── QR Code Page (for WhatsApp linking on Render) ──────────────────

pub async fn qr_page() -> impl IntoResponse {
    let qr_string = std::fs::read_to_string("/tmp/latest_qr.txt")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

    if let Some(qr) = qr_string {
        let encoded: String = qr.chars().map(|c| match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' | '~' => c.to_string(),
            _ => format!("%{:02X}", c as u32 as u8),
        }).collect();
        let img_url = format!("https://api.qrserver.com/v1/create-qr-code/?size=400x400&format=png&data={}", encoded);

        axum::response::Html(format!(r#"<!DOCTYPE html>
<html><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Professor AI — Scan to Link</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ background: #0a0a0a; color: #fff; min-height: 100vh; display: flex; flex-direction: column; align-items: center; justify-content: center; font-family: -apple-system, BlinkMacSystemFont, system-ui, sans-serif; padding: 24px; text-align: center; }}
  h1 {{ font-size: 2em; margin-bottom: 8px; }}
  .subtitle {{ color: #aaa; margin-bottom: 24px; font-size: 1.05em; }}
  .qr-wrap {{ background: #fff; border-radius: 20px; padding: 20px; display: inline-block; margin-bottom: 20px; box-shadow: 0 0 60px rgba(37,211,102,0.3); }}
  .qr-wrap img {{ display: block; width: 280px; height: 280px; }}
  .timer {{ color: #25D366; font-size: 1.1em; font-weight: bold; margin-bottom: 8px; }}
  .hint {{ color: #666; font-size: 0.85em; max-width: 400px; line-height: 1.6; }}
  .steps {{ background: #111; border-radius: 12px; padding: 16px 24px; margin: 20px 0; text-align: left; max-width: 400px; }}
  .steps li {{ color: #ccc; margin: 6px 0; font-size: 0.9em; }}
  .bar {{ width: 280px; height: 4px; background: #222; border-radius: 4px; margin: 0 auto 8px; overflow: hidden; }}
  .fill {{ height: 100%; background: #25D366; animation: drain 28s linear forwards; }}
  @keyframes drain {{ from {{ width: 100%; }} to {{ width: 0%; }} }}
</style>
</head>
<body>
<h1>🎓 Professor AI</h1>
<p class="subtitle">Scan with WhatsApp to connect your AI assistant</p>
<div class="qr-wrap">
  <img id="qrimg" src="{img_url}" alt="WhatsApp QR Code">
</div>
<div class="bar"><div class="fill" id="fill"></div></div>
<p class="timer" id="timer">⏱ Refreshing in <span id="secs">28</span>s</p>
<ol class="steps">
  <li>Open <strong>WhatsApp</strong> on your phone</li>
  <li>Tap <strong>⋮ Menu → Linked Devices</strong></li>
  <li>Tap <strong>Link a Device</strong></li>
  <li>Point camera at the QR code above</li>
</ol>
<p class="hint">The QR code expires every 30 seconds. This page auto-refreshes it for you — just scan quickly once it appears!</p>
<script>
  let secs = 28;
  const secsEl = document.getElementById('secs');
  const timer = setInterval(() => {{
    secs--;
    secsEl.textContent = secs;
    if (secs <= 0) {{
      clearInterval(timer);
      window.location.reload();
    }}
  }}, 1000);
</script>
</body></html>"#))
    } else {
        axum::response::Html(r#"<!DOCTYPE html>
<html><head>
<meta charset="utf-8">
<meta http-equiv="refresh" content="5">
<title>Professor AI</title>
<style>
  body {{ background: #0a0a0a; color: #fff; display: flex; flex-direction: column; align-items: center; justify-content: center; min-height: 100vh; font-family: system-ui; margin: 0; text-align: center; padding: 24px; }}
  h1 {{ font-size: 2.5em; margin-bottom: 12px; }}
  p {{ color: #aaa; max-width: 480px; line-height: 1.7; }}
  .dot {{ animation: pulse 1s infinite; display: inline-block; }}
  @keyframes pulse {{ 0%,100%{{ opacity:1; }} 50%{{ opacity:0.3; }} }}
</style>
</head>
<body>
<h1>⏳ Generating QR<span class="dot">...</span></h1>
<p>WhatsApp takes a few seconds to generate a fresh QR code after a restart.<br><strong>This page will auto-refresh every 5 seconds</strong> until the code is ready.</p>
</body></html>"#.to_string())
    }
}

