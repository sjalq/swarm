use crate::state::{RemoteData, Stats, WsState};
use leptos::prelude::*;

#[component]
pub fn Header(
    stats: RwSignal<RemoteData<Stats>>,
    ws_state: RwSignal<WsState>,
) -> impl IntoView {
    view! {
        <header class="header">
            <div class="header-left">
                <span class="header-title">"swarm"</span>
                <StatsBar stats=stats />
            </div>
            <div class="header-right">
                <ConnectionIndicator ws_state=ws_state />
            </div>
        </header>
    }
}

#[component]
fn StatsBar(stats: RwSignal<RemoteData<Stats>>) -> impl IntoView {
    move || {
        let data = stats.get();
        match data {
            RemoteData::Success(s) => {
                view! {
                    <div class="stats-bar">
                        <div class="stat">
                            <span class="stat-value">{s.alive}</span>
                            <span class="stat-label">"alive"</span>
                        </div>
                        <div class="stat">
                            <span class="stat-value">{s.total}</span>
                            <span class="stat-label">"total"</span>
                        </div>
                        <div class="stat">
                            <span class="stat-value">{s.messages}</span>
                            <span class="stat-label">"msgs"</span>
                        </div>
                        <div class="stat">
                            <span class="stat-value">{s.errors}</span>
                            <span class="stat-label">"errs"</span>
                        </div>
                    </div>
                }
                .into_any()
            }
            RemoteData::Loading => {
                view! { <div class="stats-bar"><span class="stat-label">"loading..."</span></div> }
                    .into_any()
            }
            _ => view! { <div class="stats-bar"></div> }.into_any(),
        }
    }
}

#[component]
fn ConnectionIndicator(ws_state: RwSignal<WsState>) -> impl IntoView {
    let dot_class = move || {
        format!("connection-dot {}", ws_state.get().css_class())
    };
    let label = move || ws_state.get().label();

    view! {
        <div class="connection-indicator">
            <div class=dot_class></div>
            <span>{label}</span>
        </div>
    }
}
