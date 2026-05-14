package org.emathp.ratelimit;

import java.util.concurrent.atomic.AtomicLong;

/** Deterministic monotonic clock for tests (not thread-safe for concurrent writes unless noted). */
final class FakeNanoClock implements TimeSource {

    private final AtomicLong nanos = new AtomicLong();

    @Override
    public long nanoTime() {
        return nanos.get();
    }

    void setNanos(long value) {
        nanos.set(value);
    }

    void advanceNanos(long delta) {
        nanos.addAndGet(delta);
    }

    void advanceMillis(long millis) {
        advanceNanos(millis * 1_000_000L);
    }
}
