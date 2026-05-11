package org.emathp.snapshot.adapters.time;

import java.time.Instant;
import org.emathp.snapshot.ports.Clock;

public final class SystemClock implements Clock {

    @Override
    public Instant now() {
        return Instant.now();
    }
}
