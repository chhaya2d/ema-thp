package org.emathp.snapshot.ports;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import org.emathp.model.EngineRow;
import org.emathp.snapshot.model.ChunkMetadata;
import org.emathp.snapshot.model.SnapshotEnvironment;

/**
 * Persistence port for snapshot trees (filesystem, in-memory, remote, etc.).
 *
 * <p>Layout convention: {@code <root>/<env>/<scopeSegment>/<queryHash>/} with per-connector
 * subdirectories. {@code scopeSegment} typically encodes tenant + role + principal (see {@link
 * org.emathp.cache.QueryCacheScope#snapshotScopeDirectoryName()}).
 */
public interface SnapshotStore {

    Path baseRoot();

    Path querySnapshotDir(SnapshotEnvironment env, String scopeSegment, String queryHash);

    void ensureQueryInfo(
            Path queryDir, String queryHash, String scopeSegment, String normalizedQuery, String createdAt)
            throws IOException;

    void deleteRecursively(Path queryDir) throws IOException;

    List<ChunkMetadata> listChunkMetasOrdered(Path connectorSnapshotDir) throws IOException;

    Optional<ChunkMetadata> latestChunkMeta(Path connectorSnapshotDir) throws IOException;

    OptionalSnapshotChunk loadLatestChunk(Path connectorSnapshotDir) throws IOException;

    void writeChunk(
            Path connectorSnapshotDir,
            int startRow,
            int endRow,
            List<EngineRow> rows,
            ChunkMetadata meta)
            throws IOException;

    List<EngineRow> readChunkData(Path connectorSnapshotDir, String prefix) throws IOException;

    /** Full materialisation: one chunk + minimal JSON manifest under {@code _materialized/}. */
    void writeMaterializedSnapshot(
            Path materializedDir, List<EngineRow> rows, ChunkMetadata meta, String manifestJsonUtf8)
            throws IOException;

    String readMaterializedManifest(Path materializedDir) throws IOException;

    List<EngineRow> readMaterializedRows(Path materializedDir, String chunkPrefix) throws IOException;

    boolean exists(Path path);

    record OptionalSnapshotChunk(ChunkMetadata meta, List<EngineRow> rows) {}
}
