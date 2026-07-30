use std::{env, fs, path::Path};

use aes_gcm::{
    Aes256Gcm, KeyInit, Nonce,
    aead::{Aead, OsRng, rand_core::RngCore},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use thiserror::Error;

const IV_LENGTH: usize = 12;
const AUTH_TAG_LENGTH: usize = 16;

#[derive(Debug, Error)]
pub enum KeyCryptoError {
    #[error("MEK is not set or invalid")]
    InvalidMek,
    #[error("Invalid ciphertext: expected non-empty base64 string")]
    InvalidCiphertext,
    #[error("Decryption failed: {0}")]
    Decryption(String),
    #[error("Failed to read MEK file: {0}")]
    MekFile(String),
}

#[derive(Clone)]
pub struct KeyCrypto {
    key: [u8; 32],
}

impl KeyCrypto {
    pub fn from_hex(mek: &str) -> Result<Self, KeyCryptoError> {
        if mek.len() != 64 || !mek.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(KeyCryptoError::InvalidMek);
        }
        let bytes = hex::decode(mek).map_err(|_| KeyCryptoError::InvalidMek)?;
        let key: [u8; 32] = bytes.try_into().map_err(|_| KeyCryptoError::InvalidMek)?;
        Ok(Self { key })
    }

    pub fn from_environment() -> Result<Self, KeyCryptoError> {
        if let Ok(mek) = env::var("MEK") {
            return Self::from_hex(mek.trim());
        }
        let path = env::var("MEK_FILE").map_err(|_| KeyCryptoError::InvalidMek)?;
        let raw = fs::read_to_string(Path::new(&path))
            .map_err(|error| KeyCryptoError::MekFile(error.to_string()))?;
        let trimmed = raw.trim();
        let mek = trimmed.strip_prefix("MEK=").unwrap_or(trimmed).trim();
        Self::from_hex(mek)
    }

    pub fn encrypt(&self, plaintext: &str) -> Result<String, KeyCryptoError> {
        let mut iv = [0_u8; IV_LENGTH];
        OsRng.fill_bytes(&mut iv);
        self.encrypt_with_iv(plaintext, iv)
    }

    fn encrypt_with_iv(
        &self,
        plaintext: &str,
        iv: [u8; IV_LENGTH],
    ) -> Result<String, KeyCryptoError> {
        let cipher =
            Aes256Gcm::new_from_slice(&self.key).map_err(|_| KeyCryptoError::InvalidMek)?;
        let encrypted = cipher
            .encrypt(Nonce::from_slice(&iv), plaintext.as_bytes())
            .map_err(|error| KeyCryptoError::Decryption(error.to_string()))?;
        let mut output = Vec::with_capacity(IV_LENGTH + encrypted.len());
        output.extend_from_slice(&iv);
        output.extend_from_slice(&encrypted);
        Ok(STANDARD.encode(output))
    }

    pub fn decrypt(&self, ciphertext_base64: &str) -> Result<String, KeyCryptoError> {
        if ciphertext_base64.is_empty() {
            return Err(KeyCryptoError::InvalidCiphertext);
        }
        let data = STANDARD
            .decode(ciphertext_base64)
            .map_err(|error| KeyCryptoError::Decryption(error.to_string()))?;
        if data.len() < IV_LENGTH + AUTH_TAG_LENGTH {
            return Err(KeyCryptoError::Decryption(
                "Ciphertext too short".to_owned(),
            ));
        }
        let (iv, encrypted) = data.split_at(IV_LENGTH);
        let cipher =
            Aes256Gcm::new_from_slice(&self.key).map_err(|_| KeyCryptoError::InvalidMek)?;
        let decrypted = cipher
            .decrypt(Nonce::from_slice(iv), encrypted)
            .map_err(|error| KeyCryptoError::Decryption(error.to_string()))?;
        String::from_utf8(decrypted).map_err(|error| KeyCryptoError::Decryption(error.to_string()))
    }

    pub fn smoke_test(&self) -> bool {
        self.encrypt("smoke-test-value")
            .and_then(|encrypted| self.decrypt(&encrypted))
            .is_ok_and(|plaintext| plaintext == "smoke-test-value")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_MEK: &str = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";

    #[test]
    fn decrypts_node_aes_256_gcm_fixture_exactly() {
        let crypto = KeyCrypto::from_hex(TEST_MEK).expect("MEK");
        assert_eq!(
            crypto
                .decrypt("AAECAwQFBgcICQoLNXelb+iVo2nkNe6mwowbH+ailtd9hSPIkFGut85Q46Jf/A==")
                .expect("Node fixture"),
            "rust-parity-secret"
        );
    }

    #[test]
    fn fixed_iv_encryption_matches_node_fixture() {
        let crypto = KeyCrypto::from_hex(TEST_MEK).expect("MEK");
        assert_eq!(
            crypto
                .encrypt_with_iv("rust-parity-secret", [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11])
                .expect("encrypt"),
            "AAECAwQFBgcICQoLNXelb+iVo2nkNe6mwowbH+ailtd9hSPIkFGut85Q46Jf/A=="
        );
    }

    #[test]
    fn rejects_invalid_mek_and_tampered_ciphertext() {
        assert!(matches!(
            KeyCrypto::from_hex("not-a-key"),
            Err(KeyCryptoError::InvalidMek)
        ));
        let crypto = KeyCrypto::from_hex(TEST_MEK).expect("MEK");
        assert!(matches!(
            crypto.decrypt("AAECAwQFBgcICQoLNXelb+iVo2nkNe6mwowbH+ailtd9hSPIkFGut85Q46Jf/B=="),
            Err(KeyCryptoError::Decryption(_))
        ));
    }

    #[test]
    fn smoke_test_round_trips() {
        assert!(KeyCrypto::from_hex(TEST_MEK).expect("MEK").smoke_test());
    }
}
