pub mod casual_mechanics;
pub mod coordination;
pub mod match_facts;
pub mod match_lifecycle;
pub mod private_identity;
// Intentionally not compiled. The migration-109 implementation duplicates
// canonical match_player_* facts and must not become a production worker path.
// Reintroduce this module only after it projects the shared canonical facts
// into physically separate casual aggregate tables.
pub mod profile_enrichment;
pub mod relay;
pub mod requested_match;
pub mod scheduler;
pub mod scheduler_runtime;
pub mod tier_stats;
