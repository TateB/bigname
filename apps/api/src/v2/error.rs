use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ErrorCode {
    InvalidInput,
    NotFound,
    Unsupported,
    Stale,
    Conflict,
    InternalError,
}

impl ErrorCode {
    pub(crate) fn http_status(&self) -> StatusCode {
        match self {
            Self::InvalidInput => StatusCode::BAD_REQUEST,
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::Unsupported => StatusCode::UNPROCESSABLE_ENTITY,
            Self::Stale | Self::Conflict => StatusCode::CONFLICT,
            Self::InternalError => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    pub(crate) fn wire(&self) -> &'static str {
        match self {
            Self::InvalidInput => "invalid_input",
            Self::NotFound => "not_found",
            Self::Unsupported => "unsupported",
            Self::Stale => "stale",
            Self::Conflict => "conflict",
            Self::InternalError => "internal_error",
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct ErrorEnvelope {
    pub(crate) error: ErrorBody,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub(crate) struct ErrorBody {
    pub(crate) code: String,
    pub(crate) message: String,
    #[serde(default)]
    pub(crate) details: Map<String, Value>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct V2Error {
    code: ErrorCode,
    message: String,
    details: Map<String, Value>,
}

impl V2Error {
    pub(crate) fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            details: Map::new(),
        }
    }

    pub(crate) fn invalid_input(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::InvalidInput, message)
    }

    pub(crate) fn not_found(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::NotFound, message)
    }

    pub(crate) fn unsupported(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Unsupported, message)
    }

    pub(crate) fn stale(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Stale, message)
    }

    pub(crate) fn conflict(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Conflict, message)
    }

    pub(crate) fn internal_error(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::InternalError, message)
    }

    #[cfg(test)]
    pub(crate) fn with_details(mut self, details: Map<String, Value>) -> Self {
        self.details = details;
        self
    }

    #[cfg(test)]
    pub(crate) fn code(&self) -> ErrorCode {
        self.code
    }

    pub(crate) fn envelope(&self) -> ErrorEnvelope {
        ErrorEnvelope {
            error: ErrorBody {
                code: self.code.wire().to_owned(),
                message: self.message.clone(),
                details: self.details.clone(),
            },
        }
    }
}

impl IntoResponse for V2Error {
    fn into_response(self) -> Response {
        (self.code.http_status(), Json(self.envelope())).into_response()
    }
}

pub(crate) type V2Result<T> = std::result::Result<T, V2Error>;

#[cfg(test)]
mod tests {
    use axum::response::IntoResponse;
    use serde_json::json;

    use super::*;

    #[test]
    fn error_codes_map_to_http_status_and_wire_string() {
        let cases = [
            (
                ErrorCode::InvalidInput,
                StatusCode::BAD_REQUEST,
                "invalid_input",
            ),
            (ErrorCode::NotFound, StatusCode::NOT_FOUND, "not_found"),
            (
                ErrorCode::Unsupported,
                StatusCode::UNPROCESSABLE_ENTITY,
                "unsupported",
            ),
            (ErrorCode::Stale, StatusCode::CONFLICT, "stale"),
            (ErrorCode::Conflict, StatusCode::CONFLICT, "conflict"),
            (
                ErrorCode::InternalError,
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
            ),
        ];

        for (code, status, wire) in cases {
            assert_eq!(code.http_status(), status);
            assert_eq!(code.wire(), wire);
        }
    }

    #[test]
    fn error_response_serializes_transport_envelope_shape() {
        let mut details = Map::new();
        details.insert("field".to_owned(), json!("page_size"));
        let error =
            V2Error::invalid_input("page_size must be between 1 and 200").with_details(details);

        let value = serde_json::to_value(error.envelope()).expect("error must serialize");

        assert_eq!(
            value,
            json!({
                "error": {
                    "code": "invalid_input",
                    "message": "page_size must be between 1 and 200",
                    "details": {
                        "field": "page_size"
                    }
                }
            })
        );
    }

    #[test]
    fn error_into_response_uses_mapped_http_status() {
        let response = V2Error::unsupported("unsupported option").into_response();

        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }
}
