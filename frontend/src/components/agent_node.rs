use crate::components::chat_panel::ChatPanel;
use crate::state::{format_relative_time, format_timestamp, AgentTreeNode};
use leptos::prelude::*;

#[component]
pub fn AgentNode(node: AgentTreeNode) -> AnyView {
    let expanded = RwSignal::new(false);
    let show_chat = RwSignal::new(false);
    let agent = node.agent.clone();
    let children = node.children;
    let last_activity = node.last_activity;
    let has_children = !children.is_empty();

    let status_class = agent.status_class().to_string();
    let harness_class = agent.harness_class().to_string();
    let model_display = agent.display_model().to_string();
    let harness_display = agent.harness.clone();
    let role = agent.role.clone();
    let id = agent.id.clone();
    let created_at = agent.created_at.clone();

    let card_class = move || {
        if expanded.get() {
            "agent-card expanded"
        } else {
            "agent-card"
        }
    };

    let on_click = move |_| {
        if expanded.get_untracked() {
            expanded.set(false);
            show_chat.set(false);
        } else {
            expanded.set(true);
        }
    };

    let created_display = format_timestamp(&created_at);
    let activity_display = format_relative_time(&last_activity);

    let child_prefix = if has_children { "^ " } else { "" };
    let role_display = format!("{}{}", child_prefix, role);

    let on_tab_details = move |ev: web_sys::MouseEvent| {
        ev.stop_propagation();
        show_chat.set(false);
    };
    let on_tab_chat = move |ev: web_sys::MouseEvent| {
        ev.stop_propagation();
        show_chat.set(true);
    };

    let detail_agent_id = RwSignal::new(agent.id.clone());
    let detail_prompt = RwSignal::new(agent.system_prompt.clone());
    let detail_status = RwSignal::new(agent.status.clone());
    let detail_comms = RwSignal::new(agent.comms.clone());
    let detail_workdir = RwSignal::new(agent.work_dir.clone());
    let detail_branch = RwSignal::new(agent.worktree_branch.clone());

    let detail_view = move || {
        if !expanded.get() {
            return view! { <div style="display:none"></div> }.into_any();
        }

        let details_tab_class = move || {
            if show_chat.get() { "chat-tab" } else { "chat-tab active" }
        };
        let chat_tab_class = move || {
            if show_chat.get() { "chat-tab active" } else { "chat-tab" }
        };

        let content = move || {
            if show_chat.get() {
                let aid = detail_agent_id.get();
                view! { <ChatPanel agent_id=aid /> }.into_any()
            } else {
                let prompt = detail_prompt.get();
                let prompt_preview = if prompt.len() > 500 {
                    format!("{}...", &prompt[..500])
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
                    <button class=details_tab_class on:click=on_tab_details>"details"</button>
                    <button class=chat_tab_class on:click=on_tab_chat>"chat"</button>
                </div>
                {content}
            </div>
        }
        .into_any()
    };

    let children_view = if has_children {
        let child_views: Vec<AnyView> = children
            .into_iter()
            .map(|child| AgentNode(AgentNodeProps { node: child }))
            .collect();

        Some(view! {
            <div class="agent-children">
                {child_views}
            </div>
        })
    } else {
        None
    };

    view! {
        <div class="agent-node">
            <div class=card_class on:click=on_click>
                <div class={format!("agent-status-indicator {}", status_class)}></div>
                <div class="agent-identity">
                    <span class="agent-role">{role_display}</span>
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
