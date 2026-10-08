use serde_json::Value;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct User {
    pub(crate) id: Uuid,
    pub(crate) configuration: Value,
}
