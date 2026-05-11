package org.emathp.snapshot.ports;

import java.time.Instant;

/** Testable time source for snapshot freshness and metadata timestamps. */
public interface Clock {

    Instant now();
}
