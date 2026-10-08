use serde_json::Value;
use std::time::Duration;

pub(crate) fn delivery_configuration(configuration: &Value) -> Result<i16, &'static str> {
    if !configuration.is_object() {
        return Err("user configuration must be an object");
    }
    let Some(delivery) = configuration.get("delivery") else {
        return Ok(5);
    };
    if !delivery.is_object() {
        return Err("delivery must be an object");
    }
    let Some(max_attempts) = delivery.get("max_attempts") else {
        return Ok(5);
    };
    let Some(max_attempts) = max_attempts.as_i64() else {
        return Err("delivery.max_attempts must be an integer between 1 and 100");
    };
    if !(1..=100).contains(&max_attempts) {
        return Err("delivery.max_attempts must be an integer between 1 and 100");
    }
    Ok(max_attempts as i16)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RetryableFailure {
    Http(u16),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DeliveryOutcome {
    Success,
    Retryable(RetryableFailure),
    PermanentFailure(RetryableFailure),
    TransportError(String),
}

impl DeliveryOutcome {
    pub(crate) fn from_http_status(status: u16) -> Self {
        if (200..300).contains(&status) {
            Self::Success
        } else if matches!(status, 408 | 429) || (500..=599).contains(&status) {
            Self::Retryable(RetryableFailure::Http(status))
        } else {
            Self::PermanentFailure(RetryableFailure::Http(status))
        }
    }

    pub(crate) fn is_retryable(&self) -> bool {
        matches!(self, Self::Retryable(_) | Self::TransportError(_))
    }
}

#[cfg(test)]
fn backoff_delay(attempt_number: i16, jitter_percent: u8) -> Duration {
    backoff_delay_with_settings(attempt_number, 5, 300, jitter_percent)
}

pub(crate) fn backoff_delay_with_settings(
    attempt_number: i16,
    base_seconds: u64,
    max_seconds: u64,
    jitter_percent: u8,
) -> Duration {
    let exponent = u32::try_from(attempt_number.saturating_sub(1))
        .unwrap_or(0)
        .min(32);
    let base_seconds = base_seconds
        .saturating_mul(2_u64.saturating_pow(exponent))
        .min(max_seconds);
    let jitter_percent = u64::from(jitter_percent.min(20));
    let jitter_seconds = base_seconds.saturating_mul(jitter_percent) / 100;
    Duration::from_secs(base_seconds.saturating_add(jitter_seconds).min(max_seconds))
}

#[cfg(test)]
mod tests {
    use super::{backoff_delay, delivery_configuration, DeliveryOutcome, RetryableFailure};
    use serde_json::json;
    use std::time::Duration;

    #[test]
    fn missing_delivery_configuration_defaults_to_five_total_attempts() {
        assert_eq!(delivery_configuration(&json!({})), Ok(5));
        assert_eq!(delivery_configuration(&json!({"delivery": {}})), Ok(5));
    }

    #[test]
    fn delivery_configuration_accepts_only_integer_limits_from_one_to_one_hundred() {
        for value in [1, 5, 100] {
            assert_eq!(
                delivery_configuration(&json!({"delivery": {"max_attempts": value}})),
                Ok(value)
            );
        }

        for value in [0, 101, -1] {
            assert!(delivery_configuration(&json!({
                "delivery": {"max_attempts": value}
            }))
            .is_err());
        }
        assert!(delivery_configuration(&json!({
            "delivery": {"max_attempts": "5"}
        }))
        .is_err());
    }

    #[test]
    fn retries_only_transient_http_statuses_and_transport_failures() {
        for status in [200, 204, 299] {
            assert_eq!(
                DeliveryOutcome::from_http_status(status),
                DeliveryOutcome::Success
            );
        }
        for status in [408, 429, 500, 599] {
            assert_eq!(
                DeliveryOutcome::from_http_status(status),
                DeliveryOutcome::Retryable(RetryableFailure::Http(status))
            );
        }
        for status in [301, 400, 404, 600, 700] {
            assert_eq!(
                DeliveryOutcome::from_http_status(status),
                DeliveryOutcome::PermanentFailure(RetryableFailure::Http(status))
            );
        }
        assert!(DeliveryOutcome::TransportError("timeout".to_owned()).is_retryable());
    }

    #[test]
    fn exponential_backoff_caps_base_delay_and_applies_bounded_jitter() {
        assert_eq!(backoff_delay(1, 0), Duration::from_secs(5));
        assert_eq!(backoff_delay(2, 20), Duration::from_secs(12));
        assert_eq!(backoff_delay(3, 0), Duration::from_secs(20));
        assert_eq!(backoff_delay(4, 20), Duration::from_secs(48));
        assert_eq!(backoff_delay(20, 20), Duration::from_secs(300));
        assert_eq!(backoff_delay(1, 100), Duration::from_secs(6));
    }
}
