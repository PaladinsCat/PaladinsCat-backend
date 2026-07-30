use time::OffsetDateTime;

const GAP_CHECK_MINUTES: &[u8] = &[5, 15, 25, 40, 50];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MinuteSchedule {
    Exact(u8),
    Every(u8),
    OneOf(&'static [u8]),
}

impl MinuteSchedule {
    fn matches(self, minute: u8) -> bool {
        match self {
            Self::Exact(expected) => minute == expected,
            Self::Every(interval) => interval > 0 && minute.is_multiple_of(interval),
            Self::OneOf(expected) => expected.contains(&minute),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HourSchedule {
    Any,
    Exact(u8),
    Every(u8),
}

impl HourSchedule {
    fn matches(self, hour: u8) -> bool {
        match self {
            Self::Any => true,
            Self::Exact(expected) => hour == expected,
            Self::Every(interval) => interval > 0 && hour.is_multiple_of(interval),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StartupPolicy {
    None,
    DurableCatchup { delay_seconds: u64 },
    Always { delay_seconds: u64 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScheduledJob {
    pub job_key: &'static str,
    pub scheduler_key: &'static str,
    pub cron_expression: &'static str,
    pub minute: MinuteSchedule,
    pub hour: HourSchedule,
    pub startup: StartupPolicy,
}

impl ScheduledJob {
    pub fn is_due(self, now: OffsetDateTime) -> bool {
        self.minute.matches(now.minute()) && self.hour.matches(now.hour())
    }
}

pub const SCHEDULED_JOBS: [ScheduledJob; 10] = [
    ScheduledJob {
        job_key: "ranked-tracker:leaderboard",
        scheduler_key: "ranked_tracker",
        cron_expression: "0 */4 * * *",
        minute: MinuteSchedule::Exact(0),
        hour: HourSchedule::Every(4),
        startup: StartupPolicy::DurableCatchup { delay_seconds: 2 },
    },
    ScheduledJob {
        job_key: "auto-ingester:discovery",
        scheduler_key: "auto_ingester",
        cron_expression: "30 * * * *",
        minute: MinuteSchedule::Exact(30),
        hour: HourSchedule::Any,
        startup: StartupPolicy::DurableCatchup { delay_seconds: 10 },
    },
    ScheduledJob {
        job_key: "auto-ingester:buffer-drain",
        scheduler_key: "auto_ingester",
        cron_expression: "*/5 * * * *",
        minute: MinuteSchedule::Every(5),
        hour: HourSchedule::Any,
        startup: StartupPolicy::Always { delay_seconds: 15 },
    },
    ScheduledJob {
        job_key: "auto-ingester:raw-buffer-retention",
        scheduler_key: "auto_ingester",
        cron_expression: "17 * * * *",
        minute: MinuteSchedule::Exact(17),
        hour: HourSchedule::Any,
        startup: StartupPolicy::Always { delay_seconds: 20 },
    },
    ScheduledJob {
        job_key: "auto-ingester:player-history-retention",
        scheduler_key: "auto_ingester",
        cron_expression: "23 * * * *",
        minute: MinuteSchedule::Exact(23),
        hour: HourSchedule::Any,
        startup: StartupPolicy::Always { delay_seconds: 25 },
    },
    ScheduledJob {
        job_key: "auto-ingester:materialized-view-refresh",
        scheduler_key: "auto_ingester",
        cron_expression: "5 * * * *",
        minute: MinuteSchedule::Exact(5),
        hour: HourSchedule::Any,
        startup: StartupPolicy::None,
    },
    ScheduledJob {
        job_key: "baseline-tracker:refresh",
        scheduler_key: "baseline_tracker",
        cron_expression: "0 3 * * *",
        minute: MinuteSchedule::Exact(0),
        hour: HourSchedule::Exact(3),
        startup: StartupPolicy::None,
    },
    ScheduledJob {
        job_key: "derived-projections:refresh",
        scheduler_key: "derived_projection_tracker",
        cron_expression: "30 3 * * *",
        minute: MinuteSchedule::Exact(30),
        hour: HourSchedule::Exact(3),
        startup: StartupPolicy::None,
    },
    ScheduledJob {
        job_key: "hourly-gap-checker:scan",
        scheduler_key: "hourly_gap_checker",
        cron_expression: "5,15,25,40,50 * * * *",
        minute: MinuteSchedule::OneOf(GAP_CHECK_MINUTES),
        hour: HourSchedule::Any,
        startup: StartupPolicy::DurableCatchup { delay_seconds: 5 },
    },
    ScheduledJob {
        job_key: "tier-stats:refresh",
        scheduler_key: "tier_stats",
        cron_expression: "15 * * * *",
        minute: MinuteSchedule::Exact(15),
        hour: HourSchedule::Any,
        startup: StartupPolicy::None,
    },
];

pub fn scheduled_jobs_for(scheduler_key: &str) -> impl Iterator<Item = &'static ScheduledJob> {
    SCHEDULED_JOBS
        .iter()
        .filter(move |job| job.scheduler_key == scheduler_key)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use time::{Date, Month, Time, UtcOffset};

    use super::*;
    use crate::workers::coordination::SCHEDULER_KEYS;

    fn at(hour: u8, minute: u8) -> OffsetDateTime {
        Date::from_calendar_date(2026, Month::July, 30)
            .expect("date")
            .with_time(Time::from_hms(hour, minute, 0).expect("time"))
            .assume_offset(UtcOffset::UTC)
    }

    #[test]
    fn inventory_covers_all_six_domains_with_unique_concrete_jobs() {
        let domains = SCHEDULED_JOBS
            .iter()
            .map(|job| job.scheduler_key)
            .collect::<BTreeSet<_>>();
        let jobs = SCHEDULED_JOBS
            .iter()
            .map(|job| job.job_key)
            .collect::<BTreeSet<_>>();
        assert_eq!(domains, SCHEDULER_KEYS.into_iter().collect());
        assert_eq!(jobs.len(), SCHEDULED_JOBS.len());
        assert_eq!(scheduled_jobs_for("auto_ingester").count(), 5);
    }

    #[test]
    fn native_due_edges_match_the_typescript_cron_inventory() {
        let due = |hour, minute| {
            SCHEDULED_JOBS
                .iter()
                .filter(|job| job.is_due(at(hour, minute)))
                .map(|job| job.job_key)
                .collect::<BTreeSet<_>>()
        };
        assert!(due(8, 0).contains("ranked-tracker:leaderboard"));
        assert!(!due(9, 0).contains("ranked-tracker:leaderboard"));
        assert!(due(3, 0).contains("baseline-tracker:refresh"));
        assert!(due(3, 30).contains("derived-projections:refresh"));
        assert!(due(12, 30).contains("auto-ingester:discovery"));
        assert!(due(12, 17).contains("auto-ingester:raw-buffer-retention"));
        assert!(due(12, 23).contains("auto-ingester:player-history-retention"));
        assert!(due(12, 25).contains("auto-ingester:buffer-drain"));
        assert!(due(12, 40).contains("hourly-gap-checker:scan"));
        assert!(due(12, 15).contains("tier-stats:refresh"));
    }
}
