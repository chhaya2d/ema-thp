package org.emathp.connector.mock;

import java.util.List;

/**
 * Canonical mock principal ids for the demo HTML control. Google mock accepts any name on files
 * (owner/modifiers); Notion uses explicit slices for these ids and a hash fallback for other
 * non-blank ids.
 */
public final class MockDemoUsers {

    private MockDemoUsers() {}

    /**
     * Principals that appear as owners or modifiers on mock Drive files; Notion assigns disjoint
     * slices for the first two and single-page slots for the rest (see {@link
     * org.emathp.connector.notion.mock.MockNotionApi}).
     */
    public static final List<String> CHOICES = List.of("alice", "bob", "carol", "dave", "eve", "frank");

    /** HTML {@code <option>} elements (no outer {@code <select>}). */
    public static String htmlOptionElements() {
        StringBuilder sb = new StringBuilder();
        sb.append("<option value=\"\">anonymous (full fixture)</option>");
        for (String u : CHOICES) {
            sb.append("<option value=\"")
                    .append(u)
                    .append("\">")
                    .append(u)
                    .append("</option>");
        }
        return sb.toString();
    }
}
