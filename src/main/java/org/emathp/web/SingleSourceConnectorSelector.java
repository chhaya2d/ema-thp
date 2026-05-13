package org.emathp.web;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.emathp.connector.Connector;

/**
 * Maps single-source SQL {@code FROM} table names to the connector instances registered on the web
 * runner. {@code resources} (or blank/null in model) fans out to every connector in registration
 * order; named tables target one provider.
 */
public final class SingleSourceConnectorSelector {

    private SingleSourceConnectorSelector() {}

    /**
     * @param fromTable normalized lowercase table from the parser; {@code resources} means all
     *     connectors
     */
    public static List<Connector> connectorsForSingleSource(
            String fromTable, List<Connector> registrationOrder, Map<String, Connector> connectorsByName) {

        String key =
                fromTable == null || fromTable.isBlank()
                        ? "resources"
                        : fromTable.trim().toLowerCase(Locale.ROOT);

        if ("resources".equals(key)) {
            return List.copyOf(registrationOrder);
        }

        List<Connector> matches = new ArrayList<>();
        for (Connector c : registrationOrder) {
            if (matchesTable(key, c, connectorsByName)) {
                matches.add(c);
            }
        }
        if (matches.isEmpty()) {
            throw new IllegalArgumentException(
                    "No connector matches FROM table \""
                            + key
                            + "\". Use: resources (all sides), notion, google, google-drive, drive.");
        }
        return matches;
    }

    private static boolean matchesTable(String normalizedTable, Connector c, Map<String, Connector> byName) {
        Connector keyed = byName.get(normalizedTable);
        if (keyed == c) {
            return true;
        }
        String src = c.source().toLowerCase(Locale.ROOT);
        return switch (normalizedTable) {
            case "notion" -> "notion".equals(src);
            case "google", "google-drive", "googledrive", "drive" -> "google-drive".equals(src);
            default -> src.replace("-", "").equals(normalizedTable.replace("-", ""));
        };
    }
}
