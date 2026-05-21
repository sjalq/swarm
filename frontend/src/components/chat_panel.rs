use crate::api;
use crate::state::{format_timestamp, LogEntry};
use leptos::prelude::*;
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::spawn_local;

#[component]
pub fn TopicLogPanel(
    agent_id: String,
    scroll_positions: RwSignal<HashMap<String, i32>>,
    log_cache: RwSignal<HashMap<String, Vec<LogEntry>>>,
) -> impl IntoView {
    let cached_entries =
        log_cache.with_untracked(|cache| cache.get(&agent_id).cloned().unwrap_or_default());
    let has_cached_entries = !cached_entries.is_empty();
    let entries = RwSignal::new(cached_entries);
    let expanded_json_entries = RwSignal::new(HashSet::<u64>::new());
    let draft = RwSignal::new(String::new());
    let sending = RwSignal::new(false);
    let loading = RwSignal::new(!has_cached_entries);
    let error = RwSignal::new(None::<String>);
    let cancelled = Arc::new(AtomicBool::new(false));
    let cancel_flag = cancelled.clone();

    on_cleanup(move || {
        cancel_flag.store(true, Ordering::Relaxed);
    });

    let id = agent_id.clone();
    spawn_local(async move {
        match api::fetch_agent_log(&id, 200).await {
            Ok(log) => {
                set_log_entries(entries, id.clone(), scroll_positions, log_cache, log);
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
            if cancelled.load(Ordering::Relaxed) {
                break;
            }
            if let Ok(log) = api::fetch_agent_log(&poll_id, 200).await {
                set_log_entries(
                    entries,
                    poll_id.clone(),
                    scroll_positions,
                    log_cache,
                    log,
                );
            }
        }
    });

    let form_agent_id = agent_id.clone();

    move || {
        if loading.get() {
            return view! { <div class="chat-loading">"loading topic log..."</div> }.into_any();
        }
        if let Some(err) = error.get() {
            return view! { <div class="chat-error">{err}</div> }.into_any();
        }

        let log = entries.get();
        let send_agent_id = form_agent_id.clone();
        let on_send = move |ev: web_sys::SubmitEvent| {
            ev.prevent_default();
            if sending.get_untracked() {
                return;
            }

            let message = draft.get_untracked();
            let message = message.trim().to_string();
            if message.is_empty() {
                return;
            }

            sending.set(true);
            let to_agent = send_agent_id.clone();
            spawn_local(async move {
                match api::send_user_message(&to_agent, &message).await {
                    Ok(()) => {
                        draft.set(String::new());
                        if let Ok(log) = api::fetch_agent_log(&to_agent, 200).await {
                            set_log_entries(
                                entries,
                                to_agent.clone(),
                                scroll_positions,
                                log_cache,
                                log,
                            );
                        }
                        error.set(None);
                    }
                    Err(e) => error.set(Some(e)),
                }
                sending.set(false);
            });
        };
        let panel_agent_id = form_agent_id.clone();
        let scroll_agent_id = panel_agent_id.clone();
        let on_log_scroll = move |ev: web_sys::Event| {
            let target = event_target::<web_sys::HtmlElement>(&ev);
            let agent_id = scroll_agent_id.clone();
            scroll_positions.update(|positions| {
                positions.insert(agent_id, target.scroll_top());
            });
        };

        view! {
            <div
                class="chat-panel"
                data-agent-log-id=panel_agent_id
                on:scroll=on_log_scroll
            >
                <div class="chat-messages">
                    {if log.is_empty() {
                        view! { <div class="chat-empty">"no log entries"</div> }.into_any()
                    } else {
                        view! {
                            <>
                                {log.into_iter().map(|entry| {
                                    view! {
                                        <ChatBubble
                                            entry=entry
                                            expanded_json_entries=expanded_json_entries
                                        />
                                    }
                                }).collect::<Vec<_>>()}
                            </>
                        }
                        .into_any()
                    }}
                </div>
                <form class="topic-message-form" on:submit=on_send>
                    <input
                        class="topic-message-input"
                        type="text"
                        placeholder="message this agent"
                        prop:value=move || draft.get()
                        on:input=move |ev| draft.set(event_target_value(&ev))
                        disabled=move || sending.get()
                    />
                    <button
                        class="topic-message-send"
                        type="submit"
                        disabled=move || sending.get() || draft.with(|value| value.trim().is_empty())
                    >
                        {move || if sending.get() { "sending" } else { "send" }}
                    </button>
                </form>
            </div>
        }
        .into_any()
    }
}

fn set_log_entries(
    entries: RwSignal<Vec<LogEntry>>,
    agent_id: String,
    scroll_positions: RwSignal<HashMap<String, i32>>,
    log_cache: RwSignal<HashMap<String, Vec<LogEntry>>>,
    log: Vec<LogEntry>,
) {
    let changed = entries.with_untracked(|current| current != &log);
    if !changed {
        return;
    }

    log_cache.update(|cache| {
        cache.insert(agent_id.clone(), log.clone());
    });
    entries.set(log);
    restore_log_scroll(agent_id, scroll_positions);
}

fn restore_log_scroll(agent_id: String, scroll_positions: RwSignal<HashMap<String, i32>>) {
    let Some(scroll_top) = scroll_positions.with_untracked(|positions| {
        positions.get(&agent_id).copied()
    }) else {
        return;
    };

    spawn_local(async move {
        gloo_timers::future::TimeoutFuture::new(0).await;
        if let Some(element) = log_panel_element(&agent_id) {
            element.set_scroll_top(scroll_top);
        }
    });
}

fn log_panel_element(agent_id: &str) -> Option<web_sys::HtmlElement> {
    let selector = format!(".chat-panel[data-agent-log-id=\"{agent_id}\"]");
    web_sys::window()?
        .document()?
        .query_selector(&selector)
        .ok()
        .flatten()?
        .dyn_into::<web_sys::HtmlElement>()
        .ok()
}

#[component]
fn ChatBubble(entry: LogEntry, expanded_json_entries: RwSignal<HashSet<u64>>) -> impl IntoView {
    let bubble_class = entry.bubble_class().to_string();
    let label = entry.label();
    let timestamp = format_timestamp(&entry.timestamp);
    let content = entry.content.clone();
    let content_class = if is_json_output_entry(&entry) {
        "bubble-content json-output"
    } else {
        "bubble-content"
    };
    let is_collapsible = is_json_output_entry(&entry);
    let entry_key = log_entry_key(&entry);
    let label_title = json_summary(&content);
    let expanded =
        Memo::new(move |_| expanded_json_entries.with(|entries| entries.contains(&entry_key)));

    if is_collapsible {
        let outer_class_base = bubble_class.clone();
        let outer_class = move || {
            if expanded.get() {
                outer_class_base.clone()
            } else {
                format!("{outer_class_base} json-collapsed")
            }
        };
        let collapsed_label_class = move || {
            if expanded.get() {
                "bubble-json-label hidden"
            } else {
                "bubble-json-label"
            }
        };
        let expanded_body_class = move || {
            if expanded.get() {
                "bubble-json-expanded"
            } else {
                "bubble-json-expanded hidden"
            }
        };
        let expanded_content = format_json_output(&content);
        let collapsed_label = label.clone();
        let expanded_label = label.clone();
        let collapsed_title = label_title.clone();
        let expanded_title = label_title;
        let on_collapsed_toggle = move |ev: web_sys::MouseEvent| {
            ev.stop_propagation();
            expanded_json_entries.update(|entries| {
                if entries.contains(&entry_key) {
                    entries.remove(&entry_key);
                } else {
                    entries.insert(entry_key);
                }
            });
        };
        let on_expanded_toggle = move |ev: web_sys::MouseEvent| {
            ev.stop_propagation();
            expanded_json_entries.update(|entries| {
                if entries.contains(&entry_key) {
                    entries.remove(&entry_key);
                } else {
                    entries.insert(entry_key);
                }
            });
        };
        return view! {
            <div class=outer_class>
                <button
                    class=collapsed_label_class
                    title=collapsed_title
                    on:click=on_collapsed_toggle
                >
                    <span class="bubble-json-indicator">">"</span>
                    <span>{collapsed_label}</span>
                </button>
                <div class=expanded_body_class>
                    <div class="bubble-header">
                        <button
                            class="bubble-label bubble-json-header-label"
                            title=expanded_title
                            on:click=on_expanded_toggle
                        >
                            <span class="bubble-json-indicator">"v"</span>
                            <span>{expanded_label}</span>
                        </button>
                        <span class="bubble-time">{timestamp}</span>
                    </div>
                    <div class=content_class>{expanded_content}</div>
                </div>
            </div>
        }
        .into_any();
    }

    view! {
        <div class=bubble_class>
            <div class="bubble-header">
                <span class="bubble-label">{label}</span>
                <span class="bubble-time">{timestamp}</span>
            </div>
            <div class=content_class>{content}</div>
        </div>
    }
    .into_any()
}

fn is_json_output_entry(entry: &LogEntry) -> bool {
    matches!(
        entry.kind.as_str(),
        "output" | "interrupted" | "error" | "timeout"
    ) && looks_like_json(&entry.content)
}

fn looks_like_json(content: &str) -> bool {
    let trimmed = content.trim_start();
    if !(trimmed.starts_with('{') || trimmed.starts_with('[')) {
        return false;
    }

    if serde_json::from_str::<serde_json::Value>(trimmed).is_ok() {
        return true;
    }

    trimmed
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .take(6)
        .any(|line| serde_json::from_str::<serde_json::Value>(line).is_ok())
}

fn json_summary(content: &str) -> String {
    let chars = content.chars().count();
    let lines = content
        .lines()
        .filter(|line| !line.trim().is_empty())
        .count();
    format!("JSON output collapsed - {chars} chars, {lines} lines")
}

fn format_json_output(content: &str) -> String {
    let trimmed = content.trim();
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(trimmed) {
        return serde_json::to_string_pretty(&value).unwrap_or_else(|_| content.to_string());
    }

    let mut parsed_any = false;
    let formatted_lines: Vec<String> = content
        .lines()
        .map(|line| {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                return String::new();
            }
            match serde_json::from_str::<serde_json::Value>(trimmed) {
                Ok(value) => {
                    parsed_any = true;
                    serde_json::to_string_pretty(&value).unwrap_or_else(|_| line.to_string())
                }
                Err(_) => line.to_string(),
            }
        })
        .collect();

    if parsed_any {
        formatted_lines.join("\n")
    } else {
        content.to_string()
    }
}

fn log_entry_key(entry: &LogEntry) -> u64 {
    let mut hasher = DefaultHasher::new();
    entry.timestamp.hash(&mut hasher);
    entry.kind.hash(&mut hasher);
    entry.peer.hash(&mut hasher);
    entry.content.hash(&mut hasher);
    hasher.finish()
}
