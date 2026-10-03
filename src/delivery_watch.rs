use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliverySnapshot {
    pub run_count: usize,
    pub dead_letter_count: usize,
    pub captured_error_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliveryDecision {
    Healthy,
    Investigate { reason: String },
}

pub fn decide(snapshot: &DeliverySnapshot) -> DeliveryDecision {
    if snapshot.dead_letter_count > 0 {
        return DeliveryDecision::Investigate {
            reason: format!("{} learner deliveries are in the dead-letter queue", snapshot.dead_letter_count),
        };
    }
    if snapshot.captured_error_count > 0 {
        return DeliveryDecision::Investigate {
            reason: format!("{} course worker error groups need review", snapshot.captured_error_count),
        };
    }
    if snapshot.run_count == 0 {
        return DeliveryDecision::Investigate {
            reason: "the scheduled course delivery has no recorded runs".to_owned(),
        };
    }
    DeliveryDecision::Healthy
}

#[derive(Debug)]
pub enum WatchError {
    MissingEnvironment(&'static str),
    Transport(String),
    Infrai { status: u16, detail: String },
    InvalidResponse(String),
}

impl fmt::Display for WatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingEnvironment(name) => write!(f, "missing environment variable: {name}"),
            Self::Transport(detail) => write!(f, "transport error: {detail}"),
            Self::Infrai { status, detail } => write!(f, "Infrai request rejected ({status}): {detail}"),
            Self::InvalidResponse(detail) => write!(f, "invalid Infrai response: {detail}"),
        }
    }
}

impl std::error::Error for WatchError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dead_letters_make_a_course_delivery_actionable() {
        let snapshot = DeliverySnapshot {
            run_count: 12,
            dead_letter_count: 2,
            captured_error_count: 0,
        };

        assert_eq!(
            decide(&snapshot),
            DeliveryDecision::Investigate {
                reason: "2 learner deliveries are in the dead-letter queue".to_owned(),
            }
        );
    }
}
