/// Purpose: identify queues whose authoritative detail contains only the
/// participating human players rather than a fixed 5v5 roster.
/// Input: provider queue ID. Output: true for bot/PvE queues with variable
/// human cardinality. Relationship: shared by relay resolution, lifecycle
/// planning, and canonical fact validation so all three enforce one policy.
pub fn has_variable_human_roster(queue_id: i32) -> bool {
    matches!(queue_id, 425 | 453 | 10297 | 10348 | 10362)
}

/// Purpose: decide whether persisted roster evidence is complete enough to
/// resume without another roster fetch. Input: queue ID and observed logical
/// participant count. Output: typed completion decision shared by all workers.
pub fn roster_evidence_is_complete(queue_id: i32, player_count: usize) -> bool {
    if has_variable_human_roster(queue_id) {
        player_count > 0
    } else {
        player_count >= 10
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roster_policy_distinguishes_fixed_and_variable_queues() {
        assert!(has_variable_human_roster(425));
        assert!(roster_evidence_is_complete(425, 1));
        assert!(!roster_evidence_is_complete(424, 1));
        assert!(roster_evidence_is_complete(424, 10));
        assert!(!roster_evidence_is_complete(999_999, 5));
    }
}
