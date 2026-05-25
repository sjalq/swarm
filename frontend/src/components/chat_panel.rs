use crate::api;
use crate::state::{format_timestamp, LogEntry};
use leptos::prelude::*;
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::spawn_local;

#[component]
pub fn ChatPanel(
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
    let scroll_restore_token = Arc::new(AtomicU64::new(0));
    let cancelled = Arc::new(AtomicBool::new(false));
    let cancel_flag = cancelled.clone();

    on_cleanup(move || {
        cancel_flag.store(true, Ordering::Relaxed);
    });

    restore_saved_log_scroll(
        agent_id.clone(),
        scroll_positions,
        scroll_restore_token.clone(),
    );

    let id = agent_id.clone();
    let initial_scroll_restore_token = scroll_restore_token.clone();
    spawn_local(async move {
        match api::fetch_agent_messages(&id, 200).await {
            Ok(log) => {
                set_log_entries(
                    entries,
                    id.clone(),
                    scroll_positions,
                    log_cache,
                    initial_scroll_restore_token,
                    log,
                );
                error.set(None);
            }
            Err(e) => error.set(Some(e)),
        }
        loading.set(false);
    });

    let poll_id = agent_id.clone();
    let poll_scroll_restore_token = scroll_restore_token.clone();
    spawn_local(async move {
        loop {
            gloo_timers::future::TimeoutFuture::new(3_000).await;
            if cancelled.load(Ordering::Relaxed) {
                break;
            }
            if let Ok(log) = api::fetch_agent_messages(&poll_id, 200).await {
                set_log_entries(
                    entries,
                    poll_id.clone(),
                    scroll_positions,
                    log_cache,
                    poll_scroll_restore_token.clone(),
                    log,
                );
            }
        }
    });

    let form_agent_id = agent_id.clone();

    move || {
        if loading.get() {
            return view! { <div class="chat-loading">"loading chat..."</div> }.into_any();
        }
        if let Some(err) = error.get() {
            return view! { <div class="chat-error">{err}</div> }.into_any();
        }

        let send_agent_id = form_agent_id.clone();
        let send_scroll_restore_token = scroll_restore_token.clone();
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
            let restore_token = send_scroll_restore_token.clone();
            spawn_local(async move {
                match api::send_user_message(&to_agent, &message).await {
                    Ok(()) => {
                        draft.set(String::new());
                        if let Ok(log) = api::fetch_agent_messages(&to_agent, 200).await {
                            set_log_entries(
                                entries,
                                to_agent.clone(),
                                scroll_positions,
                                log_cache,
                                restore_token,
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
                    <For
                        each=move || entries.get()
                        key=|entry| log_entry_key(entry)
                        let(entry)
                    >
                        <ChatBubble
                            entry=entry
                            expanded_json_entries=expanded_json_entries
                        />
                    </For>
                    {move || {
                        if entries.with(|log| log.is_empty()) {
                            view! { <div class="chat-empty">"no log entries"</div> }.into_any()
                        } else {
                            view! { <div style="display:none"></div> }.into_any()
                        }
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
    scroll_restore_token: Arc<AtomicU64>,
    log: Vec<LogEntry>,
) {
    let changed = entries.with_untracked(|current| current != &log);
    if !changed {
        return;
    }

    let scroll_restore = current_scroll_restore(&agent_id, scroll_positions);
    log_cache.update(|cache| {
        cache.insert(agent_id.clone(), log.clone());
    });
    entries.set(log);
    let token = scroll_restore_token
        .fetch_add(1, Ordering::Relaxed)
        .saturating_add(1);
    restore_log_scroll(agent_id, scroll_restore, token, scroll_restore_token);
}

#[derive(Clone, Copy)]
enum ScrollRestore {
    Top(i32),
    Bottom,
}

fn restore_saved_log_scroll(
    agent_id: String,
    scroll_positions: RwSignal<HashMap<String, i32>>,
    scroll_restore_token: Arc<AtomicU64>,
) {
    let Some(scroll_top) =
        scroll_positions.with_untracked(|positions| positions.get(&agent_id).copied())
    else {
        return;
    };

    let token = scroll_restore_token
        .fetch_add(1, Ordering::Relaxed)
        .saturating_add(1);
    restore_log_scroll(
        agent_id,
        Some(ScrollRestore::Top(scroll_top)),
        token,
        scroll_restore_token,
    );
}

fn current_scroll_restore(
    agent_id: &str,
    scroll_positions: RwSignal<HashMap<String, i32>>,
) -> Option<ScrollRestore> {
    if let Some(element) = log_panel_element(agent_id) {
        let scroll_top = element.scroll_top();
        let distance_from_bottom = element
            .scroll_height()
            .saturating_sub(element.client_height())
            .saturating_sub(scroll_top);

        scroll_positions.update(|positions| {
            positions.insert(agent_id.to_string(), scroll_top);
        });

        if distance_from_bottom <= 24 {
            Some(ScrollRestore::Bottom)
        } else {
            Some(ScrollRestore::Top(scroll_top))
        }
    } else {
        scroll_positions
            .with_untracked(|positions| positions.get(agent_id).copied().map(ScrollRestore::Top))
    }
}

fn restore_log_scroll(
    agent_id: String,
    scroll_restore: Option<ScrollRestore>,
    token: u64,
    latest_token: Arc<AtomicU64>,
) {
    let Some(scroll_restore) = scroll_restore else {
        return;
    };

    spawn_local(async move {
        restore_log_scroll_after_delay(&agent_id, scroll_restore, token, latest_token.clone(), 0)
            .await;
        restore_log_scroll_after_delay(&agent_id, scroll_restore, token, latest_token, 50).await;
    });
}

async fn restore_log_scroll_after_delay(
    agent_id: &str,
    scroll_restore: ScrollRestore,
    token: u64,
    latest_token: Arc<AtomicU64>,
    delay_ms: u32,
) {
    gloo_timers::future::TimeoutFuture::new(delay_ms).await;
    if latest_token.load(Ordering::Relaxed) != token {
        return;
    }

    if let Some(element) = log_panel_element(agent_id) {
        match scroll_restore {
            ScrollRestore::Top(scroll_top) => element.set_scroll_top(scroll_top),
            ScrollRestore::Bottom => element.set_scroll_top(element.scroll_height()),
        }
    }
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
