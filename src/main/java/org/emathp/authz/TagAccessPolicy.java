package org.emathp.authz;

import java.util.Collection;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Role-based visibility over connector rows that expose an optional {@code tags} field on {@link
 * org.emathp.model.EngineRow}.
 *
 * <p>If {@link #allowedTags()} is empty, no tag-based restriction is applied (rows pass through).
 */
public record TagAccessPolicy(Set<String> allowedTags) {

    public TagAccessPolicy {
        allowedTags = allowedTags == null ? Set.of() : Set.copyOf(allowedTags);
    }

    public static TagAccessPolicy unrestricted() {
        return new TagAccessPolicy(Set.of());
    }

    public static TagAccessPolicy forAllowedTags(Collection<String> tags) {
        if (tags == null || tags.isEmpty()) {
            return unrestricted();
        }
        return new TagAccessPolicy(
                tags.stream()
                        .filter(Objects::nonNull)
                        .map(t -> t.trim().toLowerCase(Locale.ROOT))
                        .collect(Collectors.toUnmodifiableSet()));
    }

    /** Lowercase tag literals for stable intersection checks. */
    public boolean containsAllowedLiteral(String tag) {
        return allowedTags.contains(tag.trim().toLowerCase(Locale.ROOT));
    }
}
