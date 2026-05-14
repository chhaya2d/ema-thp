package org.emathp.ratelimit;

/** Which dimension of {@link RequestContext} keys a {@link TokenBucket}. */
public enum RateLimitScope {
    CONNECTOR,
    TENANT,
    USER
}
