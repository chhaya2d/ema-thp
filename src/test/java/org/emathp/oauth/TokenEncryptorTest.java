package org.emathp.oauth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class TokenEncryptorTest {

    @Test
    void roundTrip() {
        TokenEncryptor enc = TokenEncryptor.fromUtf8Passphrase("unit-test-passphrase-32chars!!");
        String plain = "ya29.access-token-example";
        String ct = enc.encrypt(plain);
        assertNotEquals(plain, ct);
        assertEquals(plain, enc.decrypt(ct));
    }

    @Test
    void ciphertextDiffersEachTime() {
        TokenEncryptor enc = TokenEncryptor.fromUtf8Passphrase("another-secret-for-gcm-iv-random");
        String a = enc.encrypt("same");
        String b = enc.encrypt("same");
        assertNotEquals(a, b);
    }

    @Test
    void utf8PassphraseRejectsBlank() {
        assertThrows(IllegalArgumentException.class, () -> TokenEncryptor.fromUtf8Passphrase("   "));
    }
}
