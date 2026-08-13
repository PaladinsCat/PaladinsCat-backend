pub mod admin;
pub mod auth;
pub mod builds;
pub mod champions;
pub mod community;
pub mod coplay;
pub mod esports;
pub(crate) mod identity;
pub mod live;
pub(crate) mod lobby_tier;
pub mod matches;
pub mod meta;
pub mod notifications;
pub mod operations;
pub mod player_ext;
pub mod players;
pub mod public_operations;
pub mod ratings;
pub mod raw_api_responses;
pub mod recovery;
pub mod reference;
pub mod search;
pub mod site_analytics;
pub mod stats;
pub mod system;
pub mod tierlists;

pub fn count_implemented_routes() -> usize {
    admin::ROUTE_COUNT
        + auth::ROUTE_COUNT
        + builds::ROUTE_COUNT
        + champions::ROUTE_COUNT
        + community::ROUTE_COUNT
        + coplay::ROUTE_COUNT
        + esports::ROUTE_COUNT
        + live::ROUTE_COUNT
        + matches::ROUTE_COUNT
        + meta::ROUTE_COUNT
        + notifications::ROUTE_COUNT
        + operations::ROUTE_COUNT
        + player_ext::ROUTE_COUNT
        + players::ROUTE_COUNT
        + public_operations::ROUTE_COUNT
        + ratings::ROUTE_COUNT
        + raw_api_responses::ROUTE_COUNT
        + recovery::ROUTE_COUNT
        + reference::ROUTE_COUNT
        + search::ROUTE_COUNT
        + site_analytics::ROUTE_COUNT
        + stats::ROUTE_COUNT
        + system::ROUTE_COUNT
        + tierlists::ROUTE_COUNT
}
