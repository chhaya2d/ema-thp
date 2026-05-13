package org.emathp.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.List;
import org.emathp.model.Query;
import org.junit.jupiter.api.Test;

class ParsedQueryNormalizerTest {

    @Test
    void selectListIdentifierCasingNormalizes() {
        Query q1 = new Query(List.of("g.Title"), null, List.of(), 3, null, null, null);
        Query q2 = new Query(List.of("g.title"), null, List.of(), 3, null, null, null);
        assertEquals(ParsedQueryNormalizer.canonical(q1), ParsedQueryNormalizer.canonical(q2));
    }

    @Test
    void limitDistinguishesKeys() {
        Query q1 = new Query(List.of(), null, List.of(), 3, null, null, null);
        Query q2 = new Query(List.of(), null, List.of(), 4, null, null, null);
        assertNotEquals(ParsedQueryNormalizer.canonical(q1), ParsedQueryNormalizer.canonical(q2));
    }

    @Test
    void cacheKeyCombinesScopeProfileAndBody() {
        String canon = ParsedQueryNormalizer.canonical(new Query(List.of("a"), null, List.of(), 1, null, null, null));
        QueryCacheScope s1 = new QueryCacheScope("u1", "t1", "r1", QueryCacheScope.CURRENT_KEY_SCHEMA);
        QueryCacheScope s2 = new QueryCacheScope("u2", "t1", "r1", QueryCacheScope.CURRENT_KEY_SCHEMA);
        String p = ParsedQueryNormalizer.webRunnerSingleProfile(5);
        String k1 = ParsedQueryNormalizer.cacheKey(s1, canon, p);
        String k2 = ParsedQueryNormalizer.cacheKey(s2, canon, p);
        assertNotEquals(k1, k2);
        assertEquals(k1, ParsedQueryNormalizer.cacheKey(s1, canon, p));
    }
}
