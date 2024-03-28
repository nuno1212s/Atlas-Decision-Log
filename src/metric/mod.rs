use atlas_metrics::MetricRegistry;

pub(crate) const DECISION_LOG_CHECKPOINT_TIME: &str = "DEC_LOG_CHECKPOINT_TIME";
pub(crate) const DECISION_LOG_CHECKPOINT_TIME_ID: usize = 900;

pub fn metrics() -> Vec<MetricRegistry> {
    vec![]
}
