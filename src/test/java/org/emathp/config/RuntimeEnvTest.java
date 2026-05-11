package org.emathp.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import org.junit.jupiter.api.Test;

class RuntimeEnvTest {

    @Test
    void parseDotEnvBodySkipsCommentsAndStripsQuotes() {
        Map<String, String> m =
                RuntimeEnv.parseDotEnvBody(
                        """
                        # ignored
                        ALPHA=one
                        BRAVO="two three"
                        CHARLIE='x'
                        """);

        assertEquals("one", m.get("ALPHA"));
        assertEquals("two three", m.get("BRAVO"));
        assertEquals("x", m.get("CHARLIE"));
    }
}
