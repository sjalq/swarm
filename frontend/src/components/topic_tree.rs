use crate::components::agent_node::{render_agent_node, NodeSignalMap};
use crate::state::{build_tree, sort_tree, Agent, AgentTreeNode, LogEntry, SortState};
use leptos::prelude::*;
use std::collections::{HashMap, HashSet};

#[component]
pub fn TopicTree(
    agents: RwSignal<Vec<Agent>>,
    activity_map: RwSignal<HashMap<String, String>>,
    sort: RwSignal<SortState>,
    show_done: RwSignal<bool>,
    loading: RwSignal<bool>,
    error: RwSignal<Option<String>>,
) -> impl IntoView {
    let expanded_agents = RwSignal::new(HashSet::<String>::new());
    let log_tabs = RwSignal::new(HashSet::<String>::new());
    let log_scroll_positions = RwSignal::new(HashMap::<String, i32>::new());
    let log_cache = RwSignal::new(HashMap::<String, Vec<LogEntry>>::new());
    let node_signals = RwSignal::new(HashMap::<String, RwSignal<AgentTreeNode>>::new());
    let root_ids = RwSignal::new(Vec::<String>::new());

    Effect::new(move |_| {
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

        let mut nodes = build_tree(&filtered, &am);
        sort_tree(&mut nodes, sort.get());
        let next_root_ids = sync_node_signals(node_signals, &nodes);

        root_ids.maybe_update(|ids| {
            if *ids == next_root_ids {
                false
            } else {
                *ids = next_root_ids;
                true
            }
        });
    });

    move || {
        if loading.get() {
            return view! { <div class="tree-loading">"loading agents..."</div> }.into_any();
        }
        if let Some(err) = error.get() {
            return view! { <div class="tree-error">{err}</div> }.into_any();
        }

        let render_empty = move || {
            if root_ids.with(|ids| ids.is_empty()) {
                view! { <div class="tree-empty">"no agents"</div> }.into_any()
            } else {
                view! { <></> }.into_any()
            }
        };

        view! {
            <div class="topic-tree">
                <For
                    each=move || root_ids.get()
                    key=|agent_id| agent_id.clone()
                    let(agent_id)
                >
                    {render_agent_node(
                        agent_id,
                        node_signals,
                        expanded_agents,
                        log_tabs,
                        log_scroll_positions,
                        log_cache,
                    )}
                </For>
                {render_empty}
            </div>
        }
        .into_any()
    }
}

fn sync_node_signals(node_signals: NodeSignalMap, nodes: &[AgentTreeNode]) -> Vec<String> {
    let mut live_ids = HashSet::<String>::new();
    let mut updates = Vec::<(String, AgentTreeNode)>::new();

    for node in nodes {
        collect_node_update(node, &mut live_ids, &mut updates);
    }

    node_signals.update(|signals| {
        signals.retain(|id, _| live_ids.contains(id));
        for (id, node) in &updates {
            signals
                .entry(id.clone())
                .or_insert_with(|| RwSignal::new(node.clone()));
        }
    });

    let signals = node_signals.get_untracked();
    for (id, node) in updates {
        if let Some(signal) = signals.get(&id).copied() {
            signal.maybe_update(move |current| {
                if current == &node {
                    false
                } else {
                    *current = node;
                    true
                }
            });
        }
    }

    nodes.iter().map(|node| node.agent.id.clone()).collect()
}

fn collect_node_update(
    node: &AgentTreeNode,
    live_ids: &mut HashSet<String>,
    updates: &mut Vec<(String, AgentTreeNode)>,
) {
    live_ids.insert(node.agent.id.clone());
    updates.push((node.agent.id.clone(), node.clone()));
    for child in &node.children {
        collect_node_update(child, live_ids, updates);
    }
}
