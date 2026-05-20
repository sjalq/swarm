use crate::api;
use crate::components::header::Header;
use crate::components::sort_controls::SortControls;
use crate::components::topic_tree::TopicTree;
use crate::state::{Agent, RemoteData, SortState, Stats, WsState};
use leptos::prelude::*;
use std::collections::HashMap;
use wasm_bindgen_futures::spawn_local;

#[component]
pub fn App() -> impl IntoView {
    let agents = RwSignal::new(Vec::<Agent>::new());
    let activity_map = RwSignal::new(HashMap::<String, String>::new());
    let stats = RwSignal::new(RemoteData::<Stats>::NotAsked);
    let ws_state = RwSignal::new(WsState::Disconnected);
    let sort = RwSignal::new(SortState::default());
    let show_done = RwSignal::new(true);
    let loading = RwSignal::new(true);
    let error = RwSignal::new(None::<String>);

    spawn_local({
        let stats = stats;
        let agents = agents;
        let loading = loading;
        let error = error;
        async move {
            loading.set(true);
            stats.set(RemoteData::Loading);

            match api::fetch_agents(true).await {
                Ok(agent_list) => {
                    agents.set(agent_list);
                    error.set(None);
                }
                Err(e) => {
                    error.set(Some(e));
                }
            }

            match api::fetch_stats().await {
                Ok(s) => stats.set(RemoteData::Success(s)),
                Err(e) => stats.set(RemoteData::Failure(e)),
            }

            loading.set(false);
        }
    });

    api::connect_websocket(agents, activity_map, ws_state);

    spawn_local({
        let stats = stats;
        async move {
            loop {
                gloo_timers::future::TimeoutFuture::new(5_000).await;
                if let Ok(s) = api::fetch_stats().await {
                    stats.set(RemoteData::Success(s));
                }
            }
        }
    });

    view! {
        <div class="app">
            <Header stats=stats ws_state=ws_state />
            <SortControls sort=sort show_done=show_done />
            <div class="main-content">
                <TopicTree
                    agents=agents
                    activity_map=activity_map
                    sort=sort
                    show_done=show_done
                    loading=loading
                    error=error
                />
            </div>
        </div>
    }
}
