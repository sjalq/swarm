use crate::components::chat_panel::ChatPanel;
use crate::state::{format_relative_time, format_timestamp, AgentTreeNode, LogEntry};
use leptos::prelude::*;
use std::collections::{HashMap, HashSet};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use wasm_bindgen_futures::spawn_local;

pub type NodeSignalMap = RwSignal<HashMap<String, RwSignal<AgentTreeNode>>>;

pub fn render_agent_node(
    agent_id: String,
    node_signals: NodeSignalMap,
    expanded_agents: RwSignal<HashSet<String>>,
    log_tabs: RwSignal<HashSet<String>>,
    log_scroll_positions: RwSignal<HashMap<String, i32>>,
    log_cache: RwSignal<HashMap<String, Vec<LogEntry>>>,
) -> AnyView {
    let Some(node) = node_signals.with_untracked(|signals| signals.get(&agent_id).copied()) else {
        return view! { <></> }.into_any();
    };

    view! {
        <AgentNode
            node=node
            node_signals=node_signals
            expanded_agents=expanded_agents
            log_tabs=log_tabs
            log_scroll_positions=log_scroll_positions
            log_cache=log_cache
        />
    }
    .into_any()
}

#[component]
pub fn AgentNode(
    node: RwSignal<AgentTreeNode>,
    node_signals: NodeSignalMap,
    expanded_agents: RwSignal<HashSet<String>>,
    log_tabs: RwSignal<HashSet<String>>,
    log_scroll_positions: RwSignal<HashMap<String, i32>>,
    log_cache: RwSignal<HashMap<String, Vec<LogEntry>>>,
) -> AnyView {
    let id = node.with_untracked(|node| node.agent.id.clone());
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

    let status_indicator_class =
        move || node.with(|node| format!("agent-status-indicator {}", node.agent.status_class()));

    let label_display = move || {
        node.with(|node| {
            let child_prefix = if node.children.is_empty() { "" } else { "^ " };
            format!("{}{}", child_prefix, node.agent.label)
        })
    };

    let harness_badge_class =
        move || node.with(|node| format!("badge badge-harness {}", node.agent.harness_class()));
    let harness_display = move || node.with(|node| node.agent.harness.clone());
    let model_display = move || node.with(|node| node.agent.display_model().to_string());
    let created_display = move || node.with(|node| format_timestamp(&node.agent.created_at));
    let last_activity_display = move || node.with(|node| format_timestamp(&node.last_activity));
    let activity_display = move || {
        activity_tick.get();
        node.with(|node| format_relative_time(&node.last_activity))
    };

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

        let content_agent_id = tab_agent_id.clone();
        let content = move || {
            if show_chat.get() {
                let aid = content_agent_id.clone();
                view! {
                    <ChatPanel
                        agent_id=aid
                        scroll_positions=log_scroll_positions
                        log_cache=log_cache
                    />
                }
                .into_any()
            } else {
                let (status, comms, workdir, branch, prompt) = node.with(|node| {
                    (
                        node.agent.status.clone(),
                        node.agent.comms.clone(),
                        node.agent.work_dir.clone(),
                        node.agent.worktree_branch.clone(),
                        node.agent.system_prompt.clone(),
                    )
                });
                let prompt_preview = if prompt.chars().count() > 500 {
                    let truncated: String = prompt.chars().take(500).collect();
                    format!("{}...", truncated)
                } else {
                    prompt
                };
                let branch_view = branch.map(|b| {
                    view! {
                        <span class="detail-key">"branch"</span>
                        <span class="detail-value">{b}</span>
                    }
                });

                view! {
                    <div class="agent-detail-grid">
                        <span class="detail-key">"status"</span>
                        <span class="detail-value">{status}</span>
                        <span class="detail-key">"comms"</span>
                        <span class="detail-value">{comms}</span>
                        <span class="detail-key">"work dir"</span>
                        <span class="detail-value">{workdir}</span>
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

    let child_ids = move || {
        node.with(|node| {
            node.children
                .iter()
                .map(|child| child.agent.id.clone())
                .collect::<Vec<_>>()
        })
    };
    let children_style = move || {
        if node.with(|node| node.children.is_empty()) {
            "display:none"
        } else {
            ""
        }
    };

    let node_agent_id = id.clone();

    view! {
        <div class="agent-node" data-agent-id=node_agent_id>
            <div class=card_class on:click=on_click>
                <div class=status_indicator_class></div>
                <div class="agent-identity">
                    <span class="agent-label">{label_display}</span>
                    <span class="agent-id">{id}</span>
                </div>
                <div class="agent-badges">
                    <span class=harness_badge_class>
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
            <div class="agent-children" style=children_style>
                <For
                    each=child_ids
                    key=|agent_id| agent_id.clone()
                    let(child_id)
                >
                    {render_agent_node(
                        child_id,
                        node_signals,
                        expanded_agents,
                        log_tabs,
                        log_scroll_positions,
                        log_cache,
                    )}
                </For>
            </div>
        </div>
    }
    .into_any()
}
