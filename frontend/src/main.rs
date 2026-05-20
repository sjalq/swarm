mod api;
mod app;
mod components;
mod state;

use app::App;
use leptos::prelude::*;

fn main() {
    mount_to_body(App);
}
