use crate::components::agent_node::{AgentNode, AgentNodeProps};
use crate::state::{build_tree, sort_tree, Agent, LogEntry, SortState};
use leptos::prelude::*;
use std::collections::{HashMap, HashSet};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use wasm_bindgen::JsCast;
use wasm_bindgen_futures::spawn_local;

#[component]
pub fn TopicTree(
    agents: RwSignal<Vec<Agent>>,
    activity_map: RwSignal<HashMap<String, String>>,
    sort: RwSignal<SortState>,
    show_done: RwSignal<bool>,
    loading: RwSignal<bool>,
    error: RwSignal<Option<String>>,
    topic_scroll_top: RwSignal<i32>,
) -> impl IntoView {
    let expanded_agents = RwSignal::new(HashSet::<String>::new());
    let log_tabs = RwSignal::new(HashSet::<String>::new());
    let log_scroll_positions = RwSignal::new(HashMap::<String, i32>::new());
    let log_cache = RwSignal::new(HashMap::<String, Vec<LogEntry>>::new());
    let scroll_restore_token = Arc::new(AtomicU64::new(0));

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
        let scroll_anchor = capture_topic_scroll_anchor(topic_scroll_top.get_untracked());
        let token = scroll_restore_token
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        restore_topic_scroll(scroll_anchor, token, scroll_restore_token.clone());

        view! {
            <div class="topic-tree">
                {nodes
                    .into_iter()
                    .map(|node| AgentNode(AgentNodeProps {
                        node,
                        expanded_agents,
                        log_tabs,
                        log_scroll_positions,
                        log_cache,
                    }))
                    .collect::<Vec<_>>()}
            </div>
        }
        .into_any()
    }
}

#[derive(Clone)]
struct TopicScrollAnchor {
    agent_id: Option<String>,
    offset_from_scroller_top: f64,
    fallback_scroll_top: i32,
}

fn capture_topic_scroll_anchor(fallback_scroll_top: i32) -> Option<TopicScrollAnchor> {
    let Some(scroller) = topic_scroll_element() else {
        return (fallback_scroll_top > 0).then_some(TopicScrollAnchor {
            agent_id: None,
            offset_from_scroller_top: 0.0,
            fallback_scroll_top,
        });
    };

    let fallback_scroll_top = scroller.scroll_top();
    let scroller_rect = scroller.get_bounding_client_rect();
    let scroller_top = scroller_rect.top();
    let scroller_bottom = scroller_rect.bottom();
    let Ok(nodes) = scroller.query_selector_all(".agent-node[data-agent-id]") else {
        return (fallback_scroll_top > 0).then_some(TopicScrollAnchor {
            agent_id: None,
            offset_from_scroller_top: 0.0,
            fallback_scroll_top,
        });
    };

    for idx in 0..nodes.length() {
        let Some(node) = nodes.item(idx) else {
            continue;
        };
        let Ok(element) = node.dyn_into::<web_sys::Element>() else {
            continue;
        };
        let rect = element.get_bounding_client_rect();
        if rect.bottom() < scroller_top || rect.top() > scroller_bottom {
            continue;
        }
        let Some(agent_id) = element.get_attribute("data-agent-id") else {
            continue;
        };
        return Some(TopicScrollAnchor {
            agent_id: Some(agent_id),
            offset_from_scroller_top: rect.top() - scroller_top,
            fallback_scroll_top,
        });
    }

    (fallback_scroll_top > 0).then_some(TopicScrollAnchor {
        agent_id: None,
        offset_from_scroller_top: 0.0,
        fallback_scroll_top,
    })
}

fn restore_topic_scroll(
    scroll_anchor: Option<TopicScrollAnchor>,
    token: u64,
    latest_token: Arc<AtomicU64>,
) {
    let Some(scroll_anchor) = scroll_anchor else {
        return;
    };
    spawn_local(async move {
        restore_topic_scroll_after_delay(&scroll_anchor, token, latest_token.clone(), 0).await;
        restore_topic_scroll_after_delay(&scroll_anchor, token, latest_token.clone(), 50).await;
        restore_topic_scroll_after_delay(&scroll_anchor, token, latest_token, 150).await;
    });
}

async fn restore_topic_scroll_after_delay(
    scroll_anchor: &TopicScrollAnchor,
    token: u64,
    latest_token: Arc<AtomicU64>,
    delay_ms: u32,
) {
    gloo_timers::future::TimeoutFuture::new(delay_ms).await;
    if latest_token.load(Ordering::Relaxed) != token {
        return;
    }

    let Some(scroller) = topic_scroll_element() else {
        return;
    };

    if let Some(agent_id) = scroll_anchor.agent_id.as_deref() {
        if let Some(element) = agent_element(agent_id) {
            let scroller_top = scroller.get_bounding_client_rect().top();
            let element_top = element.get_bounding_client_rect().top();
            let current_offset = element_top - scroller_top;
            let delta = current_offset - scroll_anchor.offset_from_scroller_top;
            let next_scroll_top = (scroller.scroll_top() as f64 + delta).round() as i32;
            scroller.set_scroll_top(next_scroll_top);
            return;
        }
    };

    scroller.set_scroll_top(scroll_anchor.fallback_scroll_top);
}

fn topic_scroll_element() -> Option<web_sys::HtmlElement> {
    web_sys::window()?
        .document()?
        .query_selector(".main-content[data-topic-scroll-root=\"true\"]")
        .ok()
        .flatten()?
        .dyn_into::<web_sys::HtmlElement>()
        .ok()
}

fn agent_element(agent_id: &str) -> Option<web_sys::Element> {
    let selector = format!(".agent-node[data-agent-id=\"{agent_id}\"]");
    web_sys::window()?
        .document()?
        .query_selector(&selector)
        .ok()
        .flatten()
}
