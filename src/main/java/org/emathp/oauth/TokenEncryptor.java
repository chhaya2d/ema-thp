package org.emathp.oauth;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;
import javax.crypto.Cipher;
import org.emathp.config.RuntimeEnv;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * AES-256-GCM envelope for token strings at rest.
 *
 * <p>NOTE: The key is derived from the {@code CONNECTOR_TOKEN_KEY} environment variable via SHA-256.
 * Production deployments should prefer a KMS-wrapped key, rotation, and external secret stores —
 * none of which are implemented here by design (see project ADR notes).
 */
public final class TokenEncryptor {

    private static final int GCM_TAG_BITS = 128;
    private static final int IV_LEN = 12;

    private final SecretKey key;

    public TokenEncryptor(byte[] rawKey32) {
        if (rawKey32.length != 32) {
            throw new IllegalArgumentException("AES-256 requires a 32-byte key");
        }
        this.key = new SecretKeySpec(rawKey32, "AES");
    }

    /**
     * Derives a 32-byte key from {@code CONNECTOR_TOKEN_KEY}. Any UTF-8 string works; it is hashed
     * to fixed width — do not rely on mnemonic strength alone for production.
     */
    public static TokenEncryptor fromConnectEnv() {
        String env = RuntimeEnv.getOrNull("CONNECTOR_TOKEN_KEY");
        if (env == null || env.isBlank()) {
            throw new IllegalStateException("CONNECTOR_TOKEN_KEY is not set (OS env or .env)");
        }
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(env.getBytes(StandardCharsets.UTF_8));
            return new TokenEncryptor(digest);
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /** Test / advanced use: key material already 32 bytes (e.g. from test secret). */
    public static TokenEncryptor fromUtf8Passphrase(String passphrase) {
        if (passphrase == null || passphrase.isBlank()) {
            throw new IllegalArgumentException("passphrase");
        }
        try {
            byte[] digest =
                    MessageDigest.getInstance("SHA-256").digest(passphrase.getBytes(StandardCharsets.UTF_8));
            return new TokenEncryptor(digest);
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    public String encrypt(String plaintext) {
        if (plaintext == null) {
            throw new IllegalArgumentException("plaintext");
        }
        try {
            byte[] iv = new byte[IV_LEN];
            new SecureRandom().nextBytes(iv);
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(GCM_TAG_BITS, iv));
            byte[] cipherBytes = cipher.doFinal(plaintext.getBytes(StandardCharsets.UTF_8));
            byte[] packed = new byte[1 + IV_LEN + cipherBytes.length];
            packed[0] = 1;
            System.arraycopy(iv, 0, packed, 1, IV_LEN);
            System.arraycopy(cipherBytes, 0, packed, 1 + IV_LEN, cipherBytes.length);
            return Base64.getEncoder().encodeToString(packed);
        } catch (javax.crypto.BadPaddingException
                | javax.crypto.IllegalBlockSizeException
                | java.security.InvalidAlgorithmParameterException
                | java.security.InvalidKeyException
                | java.security.NoSuchAlgorithmException
                | javax.crypto.NoSuchPaddingException e) {
            throw new IllegalStateException("encrypt failed", e);
        }
    }

    public String decrypt(String ciphertextB64) {
        if (ciphertextB64 == null || ciphertextB64.isBlank()) {
            throw new IllegalArgumentException("ciphertext");
        }
        try {
            byte[] packed = Base64.getDecoder().decode(ciphertextB64);
            if (packed.length < 1 + IV_LEN + 1 || packed[0] != 1) {
                throw new IllegalArgumentException("Unsupported ciphertext format");
            }
            byte[] iv = new byte[IV_LEN];
            System.arraycopy(packed, 1, iv, 0, IV_LEN);
            byte[] ct = new byte[packed.length - 1 - IV_LEN];
            System.arraycopy(packed, 1 + IV_LEN, ct, 0, ct.length);
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(GCM_TAG_BITS, iv));
            byte[] plain = cipher.doFinal(ct);
            return new String(plain, StandardCharsets.UTF_8);
        } catch (javax.crypto.BadPaddingException
                | javax.crypto.IllegalBlockSizeException
                | java.security.InvalidAlgorithmParameterException
                | java.security.InvalidKeyException
                | java.security.NoSuchAlgorithmException
                | javax.crypto.NoSuchPaddingException e) {
            throw new IllegalStateException("decrypt failed", e);
        }
    }
}
