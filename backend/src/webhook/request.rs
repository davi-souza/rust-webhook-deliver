use super::model::PayloadType;
use crate::http::error::FieldError;
use serde_json::{Map, Value};
use std::fmt;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ListWebhooksQuery {
    pub(crate) cursor: Option<Uuid>,
    pub(crate) count: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CreateWebhookBody {
    pub(crate) url: String,
    pub(crate) payload: String,
    pub(crate) payload_type: PayloadType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CreateWebhookBodyError {
    pub(crate) errors: Vec<FieldError>,
}

pub(crate) fn parse_create_webhook(
    bytes: &[u8],
) -> Result<CreateWebhookBody, CreateWebhookBodyError> {
    let value = serde_json::from_slice::<Value>(bytes).map_err(|_| CreateWebhookBodyError {
        errors: vec![FieldError::new("body", "must contain valid JSON")],
    })?;
    if !value.is_object() {
        return Err(CreateWebhookBodyError {
            errors: vec![FieldError::new("body", "must be a JSON object")],
        });
    }

    let strict_object =
        serde_json::from_slice::<StrictObject>(bytes).map_err(|_| CreateWebhookBodyError {
            errors: vec![FieldError::new("body", "must contain valid JSON")],
        })?;

    validate_object(&strict_object.object, &strict_object.duplicate_fields)
}

pub(crate) fn parse_list_webhooks_query(
    query: Option<&str>,
) -> Result<ListWebhooksQuery, Vec<FieldError>> {
    let mut cursor = None;
    let mut count = None;
    let mut errors = Vec::new();
    let mut seen_cursor = false;
    let mut seen_count = false;

    if let Some(query) = query {
        for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
            match key.as_ref() {
                "cursor" => {
                    if seen_cursor {
                        errors.push(FieldError::new("cursor", "must appear only once"));
                        continue;
                    }
                    seen_cursor = true;
                    match Uuid::parse_str(&value) {
                        Ok(value) => cursor = Some(value),
                        Err(_) => errors.push(FieldError::new("cursor", "must be a valid UUID")),
                    }
                }
                "count" => {
                    if seen_count {
                        errors.push(FieldError::new("count", "must appear only once"));
                        continue;
                    }
                    seen_count = true;
                    match value.parse::<i64>() {
                        Ok(value @ 1..=100) => count = Some(value),
                        _ => errors
                            .push(FieldError::new("count", "must be an integer from 1 to 100")),
                    }
                }
                _ => errors.push(FieldError::new(key, "is not an allowed query parameter")),
            }
        }
    }

    if !errors.is_empty() {
        return Err(errors);
    }

    Ok(ListWebhooksQuery {
        cursor,
        count: count.unwrap_or(20),
    })
}

#[derive(Debug)]
struct StrictObject {
    object: Map<String, Value>,
    duplicate_fields: Vec<String>,
}

impl<'de> serde::Deserialize<'de> for StrictObject {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct StrictObjectVisitor;

        impl<'de> serde::de::Visitor<'de> for StrictObjectVisitor {
            type Value = StrictObject;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a JSON object")
            }

            fn visit_map<A>(self, mut access: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::MapAccess<'de>,
            {
                let mut object = Map::new();
                let mut duplicate_fields = Vec::new();

                while let Some(field) = access.next_key::<String>()? {
                    let value = access.next_value::<Value>()?;
                    if object.contains_key(&field) {
                        duplicate_fields.push(field.clone());
                    }
                    object.insert(field, value);
                }

                Ok(StrictObject {
                    object,
                    duplicate_fields,
                })
            }
        }

        deserializer.deserialize_map(StrictObjectVisitor)
    }
}

fn validate_object(
    object: &Map<String, Value>,
    duplicate_fields: &[String],
) -> Result<CreateWebhookBody, CreateWebhookBodyError> {
    let mut errors = Vec::new();
    for field in object.keys() {
        if !matches!(field.as_str(), "url" | "payload" | "payload_type") {
            errors.push(FieldError::new(field, "is not an allowed field"));
        }
    }
    for field in duplicate_fields {
        errors.push(FieldError::new(field, "must appear only once"));
    }

    let url = read_required_string(object, "url", &mut errors);
    let payload = read_required_string(object, "payload", &mut errors);
    let payload_type = read_payload_type(object, &mut errors);

    if let Some(url) = &url {
        if !is_valid_webhook_url(url) {
            errors.push(FieldError::new(
                "url",
                "must be an absolute HTTP or HTTPS URL",
            ));
        }
    }

    if payload_type == Some(PayloadType::Json) {
        if let Some(payload) = &payload {
            if serde_json::from_str::<Value>(payload).is_err() {
                errors.push(FieldError::new("payload", "must contain valid JSON"));
            }
        }
    }

    if !errors.is_empty() {
        return Err(CreateWebhookBodyError { errors });
    }

    match (url, payload, payload_type) {
        (Some(url), Some(payload), Some(payload_type)) => Ok(CreateWebhookBody {
            url,
            payload,
            payload_type,
        }),
        _ => Err(CreateWebhookBodyError {
            errors: vec![FieldError::new("body", "is invalid")],
        }),
    }
}

fn read_required_string(
    object: &Map<String, Value>,
    field: &'static str,
    errors: &mut Vec<FieldError>,
) -> Option<String> {
    match object.get(field) {
        None => {
            errors.push(FieldError::new(field, "is required"));
            None
        }
        Some(Value::String(value)) => Some(value.clone()),
        Some(_) => {
            errors.push(FieldError::new(field, "must be a string"));
            None
        }
    }
}

fn read_payload_type(
    object: &Map<String, Value>,
    errors: &mut Vec<FieldError>,
) -> Option<PayloadType> {
    match object.get("payload_type") {
        None => Some(PayloadType::Json),
        Some(Value::String(value)) if value == "json" => Some(PayloadType::Json),
        Some(Value::String(value)) if value == "text" => Some(PayloadType::Text),
        Some(Value::String(_)) => {
            errors.push(FieldError::new(
                "payload_type",
                "must be either `json` or `text`",
            ));
            None
        }
        Some(_) => {
            errors.push(FieldError::new(
                "payload_type",
                "must be a string equal to `json` or `text`",
            ));
            None
        }
    }
}

fn is_valid_webhook_url(value: &str) -> bool {
    url::Url::parse(value)
        .map(|url| matches!(url.scheme(), "http" | "https") && url.host_str().is_some())
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::{parse_create_webhook, parse_list_webhooks_query};
    use crate::webhook::model::PayloadType;
    use uuid::Uuid;

    #[test]
    fn parses_valid_json_payload_and_defaults_payload_type() {
        let body = parse_create_webhook(
            br#"{"url":"https://hooks.example.com/events","payload":"{\"event\":\"created\"}"}"#,
        )
        .expect("valid body should parse");

        assert_eq!(body.url, "https://hooks.example.com/events");
        assert_eq!(body.payload, r#"{"event":"created"}"#);
        assert_eq!(body.payload_type, PayloadType::Json);
    }

    #[test]
    fn parses_text_payload_without_json_validation() {
        let body = parse_create_webhook(
            br#"{"url":"https://hooks.example.com/events","payload":"plain text","payload_type":"text"}"#,
        )
        .expect("text payload should parse");

        assert_eq!(body.payload_type, PayloadType::Text);
    }

    #[test]
    fn reports_malformed_json_as_body_error() {
        let error = parse_create_webhook(br#"{"url": "https://example.com", "payload":"}"#)
            .expect_err("malformed JSON should fail");

        assert_eq!(error.errors[0].field, "body");
    }

    #[test]
    fn aggregates_missing_wrong_type_unknown_and_semantic_field_errors() {
        let error =
            parse_create_webhook(br#"{"url":42,"payload":false,"payload_type":null,"extra":true}"#)
                .expect_err("invalid fields should fail");

        let fields = error
            .errors
            .iter()
            .map(|error| error.field.as_str())
            .collect::<Vec<_>>();
        assert!(fields.contains(&"url"));
        assert!(fields.contains(&"payload"));
        assert!(fields.contains(&"payload_type"));
        assert!(fields.contains(&"extra"));
        assert_eq!(fields.iter().filter(|field| **field == "url").count(), 1);
    }

    #[test]
    fn reports_missing_required_fields() {
        let error =
            parse_create_webhook(br#"{"payload":"{}"}"#).expect_err("missing URL should fail");

        assert!(error.errors.iter().any(|error| error.field == "url"));
    }

    #[test]
    fn rejects_duplicate_body_fields() {
        let error = parse_create_webhook(
            br#"{"url":"https://example.com","url":"https://other.example","payload":"{}"}"#,
        )
        .expect_err("duplicate fields should fail");

        assert!(error.errors.iter().any(|error| error.field == "url"));
    }

    #[test]
    fn reports_invalid_url_and_invalid_json_payload_by_field() {
        let error = parse_create_webhook(br#"{"url":"file:///tmp/payload","payload":"not json"}"#)
            .expect_err("invalid URL and JSON payload should fail");

        let fields = error
            .errors
            .iter()
            .map(|error| error.field.as_str())
            .collect::<Vec<_>>();
        assert!(fields.contains(&"url"));
        assert!(fields.contains(&"payload"));
    }

    #[test]
    fn rejects_non_object_body_and_invalid_payload_type() {
        let non_object = parse_create_webhook(br#"[]"#).expect_err("array body should fail");
        assert_eq!(non_object.errors[0].field, "body");

        let invalid_type = parse_create_webhook(
            br#"{"url":"https://example.com","payload":"{}","payload_type":"xml"}"#,
        )
        .expect_err("unknown payload type should fail");
        assert_eq!(invalid_type.errors[0].field, "payload_type");
    }

    #[test]
    fn parses_default_and_explicit_list_pagination() {
        let default = parse_list_webhooks_query(None).expect("default pagination should parse");
        assert_eq!(default.count, 20);
        assert_eq!(default.cursor, None);

        let cursor = Uuid::new_v4();
        let explicit = parse_list_webhooks_query(Some(&format!("cursor={cursor}&count=37")))
            .expect("valid cursor and count should parse");
        assert_eq!(explicit.count, 37);
        assert_eq!(explicit.cursor, Some(cursor));
    }

    #[test]
    fn rejects_invalid_cursor_and_count_values() {
        for query in ["cursor=not-a-uuid", "count=0", "count=101", "count=abc"] {
            assert!(
                parse_list_webhooks_query(Some(query)).is_err(),
                "query should be rejected: {query}"
            );
        }
    }
}
