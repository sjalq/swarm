use crate::state::{SortDirection, SortField, SortState};
use leptos::prelude::*;

#[component]
pub fn SortControls(sort: RwSignal<SortState>, show_done: RwSignal<bool>) -> impl IntoView {
    let on_sort_created = move |_| {
        sort.set(sort.get_untracked().toggle_field(SortField::CreatedAt));
    };

    let on_sort_activity = move |_| {
        sort.set(sort.get_untracked().toggle_field(SortField::LastActivity));
    };

    let on_toggle_done = move |_| {
        show_done.set(!show_done.get_untracked());
    };

    let created_class = move || {
        let s = sort.get();
        if s.field == SortField::CreatedAt {
            "sort-btn active"
        } else {
            "sort-btn"
        }
    };

    let created_label = move || {
        let s = sort.get();
        if s.field == SortField::CreatedAt {
            match s.direction {
                SortDirection::Asc => "started ^",
                SortDirection::Desc => "started v",
            }
        } else {
            "started"
        }
    };

    let activity_class = move || {
        let s = sort.get();
        if s.field == SortField::LastActivity {
            "sort-btn active"
        } else {
            "sort-btn"
        }
    };

    let activity_label = move || {
        let s = sort.get();
        if s.field == SortField::LastActivity {
            match s.direction {
                SortDirection::Asc => "activity ^",
                SortDirection::Desc => "activity v",
            }
        } else {
            "activity"
        }
    };

    let done_class = move || {
        if show_done.get() {
            "filter-toggle active"
        } else {
            "filter-toggle"
        }
    };

    view! {
        <div class="controls-bar">
            <div class="sort-controls">
                <span class="sort-label">"sort"</span>
                <button class=created_class on:click=on_sort_created>
                    {created_label}
                </button>
                <button class=activity_class on:click=on_sort_activity>
                    {activity_label}
                </button>
            </div>
            <div class="filter-controls">
                <button class=done_class on:click=on_toggle_done>
                    "show done"
                </button>
            </div>
        </div>
    }
}
