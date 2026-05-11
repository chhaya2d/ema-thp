package org.emathp.connector.google.api;

import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Normalized Drive file row shared by the mock in-memory API and {@code files.list} JSON parsing
 * in {@link org.emathp.connector.google.real.GoogleApiClient}.
 *
 * <p>Users are identified by unique usernames; {@code owner} is a single username and {@code
 * modifiers} contains distinct usernames (no duplicates).
 */
public record GoogleDriveFile(
        String id,
        String name,
        String owner,
        List<String> modifiers,
        Instant createdAt,
        Instant updatedAt,
        String webViewLink) {

    public GoogleDriveFile {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(owner, "owner");
        Objects.requireNonNull(modifiers, "modifiers");
        Objects.requireNonNull(createdAt, "createdAt");
        Objects.requireNonNull(updatedAt, "updatedAt");
        // NOTE: throwing on duplicate modifier usernames (rather than silently deduping) makes
        // accidental dupes in sample data fail loudly. Tradeoff: callers materializing this
        // record from external sources need to dedupe themselves.
        Set<String> seen = new HashSet<>();
        for (String m : modifiers) {
            Objects.requireNonNull(m, "modifier username");
            if (!seen.add(m)) {
                throw new IllegalArgumentException("Duplicate modifier username: " + m);
            }
        }
        modifiers = List.copyOf(modifiers);
    }
}
