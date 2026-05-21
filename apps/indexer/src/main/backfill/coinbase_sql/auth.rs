use std::{
    env,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use serde::Serialize;
use uuid::Uuid;

const CDP_JWT_EXPIRY_SECS: i64 = 120;
const ED25519_PKCS8_SEED_PREFIX: &[u8] = &[
    0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04, 0x20,
];

#[derive(Clone, Debug)]
pub(super) struct CoinbaseSqlAuth {
    api_key_id: String,
    signing_key: CoinbaseSqlSigningKey,
    request_host: String,
    request_path: String,
}

impl CoinbaseSqlAuth {
    pub(super) fn from_env(
        api_key_id_env: &str,
        api_key_secret_env: &str,
        request_host: String,
        request_path: String,
    ) -> Result<Self> {
        let api_key_id = read_secret_env(api_key_id_env, "Coinbase SQL API key ID")?;
        let api_key_secret = read_secret_env(api_key_secret_env, "Coinbase SQL API key secret")?;
        Self::new(api_key_id, api_key_secret, request_host, request_path)
    }

    fn new(
        api_key_id: String,
        api_key_secret: String,
        request_host: String,
        request_path: String,
    ) -> Result<Self> {
        if request_host.trim().is_empty() {
            bail!("Coinbase SQL auth request host is empty");
        }
        if !request_path.starts_with('/') {
            bail!("Coinbase SQL auth request path must start with /");
        }
        let signing_key = CoinbaseSqlSigningKey::from_secret(&api_key_secret)?;
        Ok(Self {
            api_key_id,
            signing_key,
            request_host,
            request_path,
        })
    }

    pub(super) fn bearer_token(&self) -> Result<String> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before Unix epoch; cannot build Coinbase SQL JWT")?
            .as_secs() as i64;
        let mut header = Header::new(self.signing_key.algorithm());
        header.kid = Some(self.api_key_id.clone());
        header.nonce = Some(Uuid::new_v4().simple().to_string());
        let claims = CoinbaseSqlJwtClaims {
            sub: &self.api_key_id,
            iss: "cdp",
            uris: vec![format!("POST {}{}", self.request_host, self.request_path)],
            iat: now,
            nbf: now,
            exp: now + CDP_JWT_EXPIRY_SECS,
        };

        encode(&header, &claims, self.signing_key.encoding_key())
            .context("failed to generate Coinbase SQL bearer token from Secret API Key")
    }
}

fn read_secret_env(env_name: &str, label: &str) -> Result<String> {
    let value =
        env::var(env_name).with_context(|| format!("missing {label} env var {env_name}"))?;
    let value = value.trim();
    if value.is_empty() {
        bail!("{label} env var {env_name} is empty");
    }

    Ok(value.to_owned())
}

#[derive(Clone, Debug)]
enum CoinbaseSqlSigningKey {
    Ed25519(EncodingKey),
    Ecdsa(EncodingKey),
}

impl CoinbaseSqlSigningKey {
    fn from_secret(secret: &str) -> Result<Self> {
        if let Some(key) = ed25519_key_from_base64_secret(secret)? {
            return Ok(Self::Ed25519(key));
        }

        let pem_secret = secret.replace("\\n", "\n");
        if pem_secret.contains("BEGIN") {
            if let Ok(key) = EncodingKey::from_ec_pem(pem_secret.as_bytes()) {
                return Ok(Self::Ecdsa(key));
            }
            if let Ok(key) = EncodingKey::from_ed_pem(pem_secret.as_bytes()) {
                return Ok(Self::Ed25519(key));
            }
        }

        bail!(
            "invalid Coinbase SQL API key secret format; expected a base64 Ed25519 key or PEM EC/Ed25519 private key"
        );
    }

    fn algorithm(&self) -> Algorithm {
        match self {
            Self::Ed25519(_) => Algorithm::EdDSA,
            Self::Ecdsa(_) => Algorithm::ES256,
        }
    }

    fn encoding_key(&self) -> &EncodingKey {
        match self {
            Self::Ed25519(key) | Self::Ecdsa(key) => key,
        }
    }
}

fn ed25519_key_from_base64_secret(secret: &str) -> Result<Option<EncodingKey>> {
    let Ok(decoded) = STANDARD.decode(secret.trim()) else {
        return Ok(None);
    };
    if decoded.len() != 64 {
        bail!(
            "invalid Coinbase SQL Ed25519 API key secret length {}; expected 64 decoded bytes",
            decoded.len()
        );
    }

    let mut pkcs8 = Vec::with_capacity(ED25519_PKCS8_SEED_PREFIX.len() + 32);
    pkcs8.extend_from_slice(ED25519_PKCS8_SEED_PREFIX);
    pkcs8.extend_from_slice(&decoded[..32]);
    Ok(Some(EncodingKey::from_ed_der(&pkcs8)))
}

#[derive(Serialize)]
struct CoinbaseSqlJwtClaims<'a> {
    sub: &'a str,
    iss: &'static str,
    uris: Vec<String>,
    iat: i64,
    nbf: i64,
    exp: i64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use jsonwebtoken::decode_header;
    use serde_json::Value;

    #[test]
    fn generated_token_matches_cdp_rest_jwt_shape() {
        let secret = STANDARD.encode([7u8; 64]);
        let auth = CoinbaseSqlAuth::new(
            "organizations/org/apiKeys/key".to_owned(),
            secret,
            "api.cdp.coinbase.com".to_owned(),
            "/platform/v2/data/query/run".to_owned(),
        )
        .expect("test auth should build");

        let token = auth.bearer_token().expect("token should sign");
        let header = decode_header(&token).expect("header should decode");
        assert_eq!(header.alg, Algorithm::EdDSA);
        assert_eq!(header.kid.as_deref(), Some("organizations/org/apiKeys/key"));
        assert_eq!(header.typ.as_deref(), Some("JWT"));
        assert!(header.nonce.is_some());

        let claims = decode_claims(&token);
        assert_eq!(claims["sub"], "organizations/org/apiKeys/key");
        assert_eq!(claims["iss"], "cdp");
        assert_eq!(
            claims["uris"][0],
            "POST api.cdp.coinbase.com/platform/v2/data/query/run"
        );
        assert!(claims["exp"].as_i64().unwrap() > claims["iat"].as_i64().unwrap());
        assert_eq!(claims.get("aud"), None);
    }

    #[test]
    fn invalid_base64_ed25519_secret_length_is_rejected() {
        let error = CoinbaseSqlAuth::new(
            "key".to_owned(),
            STANDARD.encode([1u8; 16]),
            "api.cdp.coinbase.com".to_owned(),
            "/platform/v2/data/query/run".to_owned(),
        )
        .expect_err("short Ed25519 secret must be rejected");

        assert!(format!("{error:#}").contains("expected 64 decoded bytes"));
    }

    fn decode_claims(token: &str) -> Value {
        let payload = token
            .split('.')
            .nth(1)
            .expect("token should contain payload");
        let decoded = URL_SAFE_NO_PAD
            .decode(payload)
            .expect("payload should be base64url");
        serde_json::from_slice(&decoded).expect("payload should be JSON")
    }
}
