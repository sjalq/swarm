use crate::components::chat_panel::ChatPanel;
use crate::state::{format_relative_time, format_timestamp, AgentTreeNode, LogEntry};
use leptos::prelude::*;
use std::collections::{HashMap, HashSet};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use wasm_bindgen_futures::spawn_local;

#[component]
pub fn AgentNode(
    node: AgentTreeNode,
    expanded_agents: RwSignal<HashSet<String>>,
    log_tabs: RwSignal<HashSet<String>>,
    log_scroll_positions: RwSignal<HashMap<String, i32>>,
    log_cache: RwSignal<HashMap<String, Vec<LogEntry>>>,
) -> AnyView {
    let agent = node.agent.clone();
    let children = node.children;
    let last_activity = node.last_activity;
    let has_children = !children.is_empty();

    let status_class = agent.status_class().to_string();
    let harness_class = agent.harness_class().to_string();
    let model_display = agent.display_model().to_string();
    let harness_display = agent.harness.clone();
    let label = agent.label.clone();
    let id = agent.id.clone();
    let created_at = agent.created_at.clone();
    let expanded_id = id.clone();
    let expanded = Memo::new(move |_| expanded_agents.with(|agents| agents.contains(&expanded_id)));
    let log_tab_id = id.clone();
    let show_chat = Memo::new(move |_| log_tabs.with(|agents| agents.contains(&log_tab_id)));
    let activity_tick = RwSignal::new(0u64);
    let cancelled = Arc::new(AtomicBool::new(false));
    let cancel_flag = cancelled.clone();

    on_cleanup(move || {
        cancel_flag.store(true, Ordering::Relaxed);
    });

    spawn_local(async move {
        loop {
            gloo_timers::future::TimeoutFuture::new(1_000).await;
            if cancelled.load(Ordering::Relaxed) {
                break;
            }
            activity_tick.update(|tick| *tick = tick.saturating_add(1));
        }
    });

    let card_class = move || {
        if expanded.get() {
            "agent-card expanded"
        } else {
            "agent-card"
        }
    };

    let click_id = id.clone();
    let on_click = move |_| {
        if expanded.get_untracked() {
            expanded_agents.update(|agents| {
                agents.remove(&click_id);
            });
            log_tabs.update(|agents| {
                agents.remove(&click_id);
            });
        } else {
            expanded_agents.update(|agents| {
                agents.insert(click_id.clone());
            });
            log_tabs.update(|agents| {
                agents.insert(click_id.clone());
            });
        }
    };

    let created_display = format_timestamp(&created_at);
    let last_activity_display = format_timestamp(&last_activity);
    let activity_display = move || {
        activity_tick.get();
        format_relative_time(&last_activity)
    };

    let child_prefix = if has_children { "^ " } else { "" };
    let label_display = format!("{}{}", child_prefix, label);

    let detail_agent_id = RwSignal::new(agent.id.clone());
    let detail_prompt = RwSignal::new(agent.system_prompt.clone());
    let detail_status = RwSignal::new(agent.status.clone());
    let detail_comms = RwSignal::new(agent.comms.clone());
    let detail_workdir = RwSignal::new(agent.work_dir.clone());
    let detail_branch = RwSignal::new(agent.worktree_branch.clone());
    let tab_agent_id = id.clone();

    let detail_view = move || {
        if !expanded.get() {
            return view! { <div style="display:none"></div> }.into_any();
        }

        let chat_id = tab_agent_id.clone();
        let on_tab_chat = move |ev: web_sys::MouseEvent| {
            ev.stop_propagation();
            log_tabs.update(|agents| {
                agents.insert(chat_id.clone());
            });
        };
        let details_id = tab_agent_id.clone();
        let on_tab_details = move |ev: web_sys::MouseEvent| {
            ev.stop_propagation();
            log_tabs.update(|agents| {
                agents.remove(&details_id);
            });
        };

        let details_tab_class = move || {
            if show_chat.get() {
                "chat-tab"
            } else {
                "chat-tab active"
            }
        };
        let chat_tab_class = move || {
            if show_chat.get() {
                "chat-tab active"
            } else {
                "chat-tab"
            }
        };

        let content = move || {
            if show_chat.get() {
                let aid = detail_agent_id.get();
                view! {
                    <ChatPanel
                        agent_id=aid
                        scroll_positions=log_scroll_positions
                        log_cache=log_cache
                    />
                }
                .into_any()
            } else {
                let prompt = detail_prompt.get();
                let prompt_preview = if prompt.chars().count() > 500 {
                    let truncated: String = prompt.chars().take(500).collect();
                    format!("{}...", truncated)
                } else {
                    prompt
                };
                let branch = detail_branch.get();
                let branch_view = branch.map(|b| {
                    view! {
                        <span class="detail-key">"branch"</span>
                        <span class="detail-value">{b}</span>
                    }
                });

                view! {
                    <div class="agent-detail-grid">
                        <span class="detail-key">"status"</span>
                        <span class="detail-value">{detail_status.get()}</span>
                        <span class="detail-key">"comms"</span>
                        <span class="detail-value">{detail_comms.get()}</span>
                        <span class="detail-key">"work dir"</span>
                        <span class="detail-value">{detail_workdir.get()}</span>
                        {branch_view}
                        <span class="detail-key">"prompt"</span>
                        <span class="detail-value prompt">{prompt_preview}</span>
                    </div>
                }
                .into_any()
            }
        };

        view! {
            <div class="agent-detail">
                <div class="chat-tab-bar">
                    <button class=chat_tab_class on:click=on_tab_chat>"Chat"</button>
                    <button class=details_tab_class on:click=on_tab_details>"details"</button>
                </div>
                {content}
            </div>
        }
        .into_any()
    };

    let children_view = if has_children {
        let child_views: Vec<AnyView> = children
            .into_iter()
            .map(|child| {
                AgentNode(AgentNodeProps {
                    node: child,
                    expanded_agents,
                    log_tabs,
                    log_scroll_positions,
                    log_cache,
                })
            })
            .collect();

        Some(view! {
            <div class="agent-children">
                {child_views}
            </div>
        })
    } else {
        None
    };

    let node_agent_id = id.clone();

    view! {
        <div class="agent-node" data-agent-id=node_agent_id>
            <div class=card_class on:click=on_click>
                <div class={format!("agent-status-indicator {}", status_class)}></div>
                <div class="agent-identity">
                    <span class="agent-label">{label_display}</span>
                    <span class="agent-id">{id}</span>
                </div>
                <div class="agent-badges">
                    <span class={format!("badge badge-harness {}", harness_class)}>
                        {harness_display}
                    </span>
                    <span class="badge badge-model">{model_display}</span>
                </div>
                <div class="agent-timestamps">
                    <span class="agent-time">
                        <span class="agent-time-label">"started "</span>
                        <span class="agent-time-value">{created_display}</span>
                    </span>
                    <span class="agent-time">
                        <span class="agent-time-label">"last active "</span>
                        <span class="agent-time-value">{last_activity_display}</span>
                    </span>
                    <span class="agent-time">
                        <span class="agent-time-label">"active "</span>
                        <span class="agent-time-value">{activity_display}</span>
                    </span>
                </div>
            </div>
            {detail_view}
            {children_view}
        </div>
    }
    .into_any()
}
