package org.emathp.authz.demo;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.emathp.auth.UserContext;
import org.emathp.authz.Principal;
import org.emathp.authz.PrincipalRegistry;

/**
 * In-memory demo principals (alice / bob) for the browser playground and CLI demos. Replace with a
 * JDBC- or claims-backed {@link PrincipalRegistry} for production wiring; no other code needs to
 * change.
 */
public final class DemoPrincipalRegistry implements PrincipalRegistry {

    private static final Map<String, Principal> BY_USER;

    static {
        Map<String, Principal> m = new LinkedHashMap<>();
        m.put(
                "alice",
                new Principal(
                        "tenant-1",
                        "hr",
                        List.of("hr"),
                        Map.of("hr", Set.of("hr", "engineering"))));
        m.put(
                "bob",
                new Principal(
                        "tenant-1",
                        "engineering",
                        List.of("engineering"),
                        Map.of("engineering", Set.of("engineering"))));
        BY_USER = Map.copyOf(m);
    }

    @Override
    public Principal lookup(UserContext user) {
        if (user == null) {
            return Principal.anonymous();
        }
        String uid = user.userId();
        if (uid == null || uid.isBlank()) {
            return Principal.anonymous();
        }
        Principal p = BY_USER.get(uid.trim().toLowerCase(Locale.ROOT));
        return p != null ? p : Principal.anonymous();
    }
}
