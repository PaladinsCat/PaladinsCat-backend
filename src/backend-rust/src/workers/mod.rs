pub mod active_match_discovery;
pub mod api_headroom;
pub mod auto_ingester_scheduler;
pub mod baseline_tracker;
pub mod baseline_tracker_scheduler;
pub mod cache_warmer;
pub mod casual_mechanics;
pub mod champion_page_cache_urls;
pub mod completed_match_batching;
pub mod coordination;
pub mod derived_projection_scheduler;
pub mod discovery_control;
pub mod discovery_store;
pub mod history_retention;
pub mod hourly_gap_checker;
pub mod live_tracker;
pub mod loadouts;
pub mod maintenance;
pub mod match_facts;
pub mod match_lifecycle;
pub mod outage;
pub mod pipeline;
pub mod policy;
pub mod private_identity;
pub mod profile_enrichment;
pub mod projections;
pub mod ranked_projection;
pub mod ranked_tracker;
pub mod rating;
pub mod raw_buffer;
pub mod relay;
pub mod requested_match;
pub mod scheduler;
pub mod scheduler_host;
pub mod scheduler_runtime;
pub mod site_cache_warm_targets;
pub mod tier_stats;
pub mod worker_lock;

pub fn count_implemented_workers() -> usize {
    41
}
