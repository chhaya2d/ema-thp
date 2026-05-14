package org.emathp.connector;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.emathp.auth.UserContext;
import org.emathp.model.ConnectorQuery;
import org.emathp.model.EngineRow;
import org.emathp.model.SearchResult;

/** Test wrapper: counts {@link #search} invocations; delegates every other capability. */
public final class CountingConnector implements Connector {

    private final Connector delegate;
    private final AtomicInteger searchCount = new AtomicInteger();

    public CountingConnector(Connector delegate) {
        this.delegate = delegate;
    }

    public int searchCount() {
        return searchCount.get();
    }

    @Override
    public String source() {
        return delegate.source();
    }

    @Override
    public int defaultFetchPageSize() {
        return delegate.defaultFetchPageSize();
    }

    @Override
    public Duration defaultFreshnessTtl() {
        return delegate.defaultFreshnessTtl();
    }

    @Override
    public DataScope dataScope() {
        return delegate. dataScope();
    }

    @Override
    public CapabilitySet capabilities() {
        return delegate.capabilities();
    }

    @Override
    public SearchResult<EngineRow> search(UserContext userContext, ConnectorQuery query) {
        searchCount.incrementAndGet();
        return delegate.search(userContext, query);
    }
}
