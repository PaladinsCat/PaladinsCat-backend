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

/// Purpose: validate score evidence according to the queue participant model.
/// Input: queue ID, optional team scores, and optional winning task force.
/// Output: whether completed-match score evidence is authoritative. Relationship:
/// fixed PvP queues require a consistent score; variable-human bot/PvE queues
/// are complete from their authoritative match and roster facts.
pub fn score_evidence_is_complete(
    queue_id: i32,
    team1: Option<i32>,
    team2: Option<i32>,
    winner: Option<i32>,
) -> bool {
    if has_variable_human_roster(queue_id) && team1.is_none() && team2.is_none() && winner.is_none()
    {
        return true;
    }
    match (team1, team2, winner) {
        (Some(team1), Some(team2), Some(1)) => team1 >= 0 && team2 >= 0 && team1 > team2,
        (Some(team1), Some(team2), Some(2)) => team1 >= 0 && team2 >= 0 && team2 > team1,
        _ => false,
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

    #[test]
    fn score_policy_distinguishes_fixed_and_variable_queues() {
        assert!(score_evidence_is_complete(10297, None, None, None));
        assert!(!score_evidence_is_complete(10297, Some(2), None, None));
        assert!(!score_evidence_is_complete(
            10297,
            Some(2),
            Some(4),
            Some(1)
        ));
        assert!(score_evidence_is_complete(486, Some(4), Some(2), Some(1)));
        assert!(!score_evidence_is_complete(486, None, None, None));
        assert!(!score_evidence_is_complete(486, Some(2), Some(4), Some(1)));
    }
}
