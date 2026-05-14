package org.emathp.ratelimit;

/** Pluggable monotonic clock for tests ({@link System#nanoTime} semantics). */
@FunctionalInterface
public interface TimeSource {

    long nanoTime();

    static TimeSource system() {
        return System::nanoTime;
    }
}
