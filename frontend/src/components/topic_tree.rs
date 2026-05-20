use crate::components::agent_node::{AgentNode, AgentNodeProps};
use crate::state::{build_tree, sort_tree, Agent, SortState};
use leptos::prelude::*;
use std::collections::HashMap;

#[component]
pub fn TopicTree(
    agents: RwSignal<Vec<Agent>>,
    activity_map: RwSignal<HashMap<String, String>>,
    sort: RwSignal<SortState>,
    show_done: RwSignal<bool>,
    loading: RwSignal<bool>,
    error: RwSignal<Option<String>>,
) -> impl IntoView {
    move || {
        if loading.get() {
            return view! { <div class="tree-loading">"loading agents..."</div> }.into_any();
        }
        if let Some(err) = error.get() {
            return view! { <div class="tree-error">{err}</div> }.into_any();
        }

        let agent_list = agents.get();
        let am = activity_map.get();

        let filtered: Vec<Agent> = if show_done.get() {
            agent_list
        } else {
            agent_list
                .into_iter()
                .filter(|a| a.status != "done")
                .collect()
        };

        if filtered.is_empty() {
            return view! { <div class="tree-empty">"no agents"</div> }.into_any();
        }

        let mut nodes = build_tree(&filtered, &am);
        sort_tree(&mut nodes, sort.get());

        view! {
            <div class="topic-tree">
                {nodes
                    .into_iter()
                    .map(|node| AgentNode(AgentNodeProps { node }))
                    .collect::<Vec<_>>()}
            </div>
        }
        .into_any()
    }
}
