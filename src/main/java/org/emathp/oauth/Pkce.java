package org.emathp.oauth;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;

/**
 * RFC 7636 PKCE: {@code code_verifier} / {@code code_challenge} (S256) for OAuth public clients.
 */
public final class Pkce {

    private static final SecureRandom RANDOM = new SecureRandom();

    private Pkce() {}

    /** High-entropy URL-safe verifier (43–128 characters after encoding). */
    public static String newVerifier() {
        byte[] b = new byte[32];
        RANDOM.nextBytes(b);
        return base64UrlNoPadding(b);
    }

    /** SHA-256 challenge, base64url-encoded (no padding). */
    public static String challengeS256(String verifier) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(verifier.getBytes(StandardCharsets.US_ASCII));
            return base64UrlNoPadding(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String base64UrlNoPadding(byte[] raw) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(raw);
    }
}
