package org.emathp.connector.mock;

import org.emathp.config.MockConnectorDefaults;

/**
 * Optional simulated latency for mock connectors. Values come from compiled {@link
 * MockConnectorDefaults} for the web demo; tests use {@link #none()}.
 */
public final class MockConnectorDevSettings {

    private final int delayMillis;

    private MockConnectorDevSettings(int delayMillis) {
        this.delayMillis = delayMillis;
    }

    public static MockConnectorDevSettings none() {
        return new MockConnectorDevSettings(0);
    }

    /** Web demo: delay from {@link MockConnectorDefaults#MOCK_SEARCH_DELAY_MILLIS} (capped). */
    public static MockConnectorDevSettings compiled() {
        int ms = MockConnectorDefaults.MOCK_SEARCH_DELAY_MILLIS;
        int capped = Math.min(Math.max(0, ms), 60_000);
        return new MockConnectorDevSettings(capped);
    }

    /** Milliseconds applied before each mock {@code search} when positive. */
    public int delayMillis() {
        return delayMillis;
    }

    public void sleepBeforeMockSearch() {
        int ms = delayMillis;
        if (ms <= 0) {
            return;
        }
        ms = Math.min(ms, 120_000);
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
