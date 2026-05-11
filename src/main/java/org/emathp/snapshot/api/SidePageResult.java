package org.emathp.snapshot.api;

import java.nio.file.Path;
import java.util.List;
import org.emathp.engine.QueryExecutor;
import org.emathp.snapshot.model.ChunkMetadata;

/** Outcome of resolving one side (snapshot hit or executor + cache write). */
public record SidePageResult(
        QueryExecutor.ExecutionResult execution,
        int providerFetches,
        int continuationFetches,
        boolean snapshotReuseNoProviderCall,
        List<String> chunkFilesCreatedThisRequest,
        ChunkMetadata authoritativeChunkMeta,
        Path connectorSnapshotDir) {}
