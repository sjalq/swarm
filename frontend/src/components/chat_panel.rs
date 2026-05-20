use crate::api;
use crate::state::{format_timestamp, LogEntry};
use leptos::prelude::*;
use wasm_bindgen_futures::spawn_local;

#[component]
pub fn ChatPanel(agent_id: String) -> impl IntoView {
    let entries = RwSignal::new(Vec::<LogEntry>::new());
    let loading = RwSignal::new(true);
    let error = RwSignal::new(None::<String>);
    let cancelled = RwSignal::new(false);

    on_cleanup(move || {
        cancelled.set(true);
    });

    let id = agent_id.clone();
    spawn_local(async move {
        match api::fetch_agent_log(&id, 200).await {
            Ok(log) => {
                entries.set(log);
                error.set(None);
            }
            Err(e) => error.set(Some(e)),
        }
        loading.set(false);
    });

    let poll_id = agent_id.clone();
    spawn_local(async move {
        loop {
            gloo_timers::future::TimeoutFuture::new(3_000).await;
            if cancelled.get_untracked() {
                break;
            }
            if let Ok(log) = api::fetch_agent_log(&poll_id, 200).await {
                entries.set(log);
            }
        }
    });

    move || {
        if loading.get() {
            return view! { <div class="chat-loading">"loading messages..."</div> }.into_any();
        }
        if let Some(err) = error.get() {
            return view! { <div class="chat-error">{err}</div> }.into_any();
        }

        let log = entries.get();
        if log.is_empty() {
            return view! { <div class="chat-empty">"no messages"</div> }.into_any();
        }

        view! {
            <div class="chat-panel">
                <div class="chat-messages">
                    {log.into_iter().map(|entry| {
                        view! { <ChatBubble entry=entry /> }
                    }).collect::<Vec<_>>()}
                </div>
            </div>
        }
        .into_any()
    }
}

#[component]
fn ChatBubble(entry: LogEntry) -> impl IntoView {
    let bubble_class = entry.bubble_class().to_string();
    let label = entry.label();
    let timestamp = format_timestamp(&entry.timestamp);
    let content = entry.content.clone();

    view! {
        <div class=bubble_class>
            <div class="bubble-header">
                <span class="bubble-label">{label}</span>
                <span class="bubble-time">{timestamp}</span>
            </div>
            <div class="bubble-content">{content}</div>
        </div>
    }
}
