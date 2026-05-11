package org.emathp.snapshot.adapters.fs;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import org.emathp.model.EngineRow;
import org.emathp.snapshot.model.ChunkData;
import org.emathp.snapshot.model.ChunkMetadata;
import org.emathp.snapshot.model.QueryInfo;
import org.emathp.snapshot.layout.ChunkNaming;
import org.emathp.snapshot.model.SnapshotEnvironment;
import org.emathp.snapshot.ports.SnapshotStore;
import org.emathp.snapshot.serde.SnapshotJson;

/** Filesystem-backed {@link SnapshotStore} (default adapter). */
public final class FsSnapshotStore implements SnapshotStore {

    private static final String MATERIALIZED_MANIFEST_FILE = "manifest.json";

    private final Path baseRoot;

    public FsSnapshotStore(Path baseRoot) {
        this.baseRoot = baseRoot.normalize();
    }

    @Override
    public Path baseRoot() {
        return baseRoot;
    }

    @Override
    public Path querySnapshotDir(SnapshotEnvironment env, String userId, String queryHash) {
        String uid = userId == null ? "_" : userId;
        return baseRoot.resolve(env.dirName()).resolve(uid).resolve(queryHash);
    }

    @Override
    public void ensureQueryInfo(
            Path queryDir, String queryHash, String userId, String normalizedQuery, String createdAt)
            throws IOException {
        Files.createDirectories(queryDir);
        Path f = queryDir.resolve("query_info.json");
        if (Files.exists(f)) {
            return;
        }
        QueryInfo info = new QueryInfo(queryHash, userId, normalizedQuery, createdAt);
        SnapshotJson.mapper().writeValue(f.toFile(), info);
    }

    @Override
    public void deleteRecursively(Path queryDir) throws IOException {
        if (!Files.exists(queryDir)) {
            return;
        }
        try (var walk = Files.walk(queryDir)) {
            List<Path> paths = walk.sorted(Comparator.reverseOrder()).toList();
            for (Path p : paths) {
                Files.deleteIfExists(p);
            }
        }
    }

    @Override
    public List<ChunkMetadata> listChunkMetasOrdered(Path connectorSnapshotDir) throws IOException {
        if (!Files.exists(connectorSnapshotDir)) {
            return List.of();
        }
        List<ChunkMetadata> metas = new ArrayList<>();
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(connectorSnapshotDir, "*_meta.json")) {
            for (Path p : ds) {
                ChunkMetadata m = SnapshotJson.mapper().readValue(p.toFile(), ChunkMetadata.class);
                metas.add(m);
            }
        }
        metas.sort(Comparator.comparingInt(ChunkMetadata::endRow));
        return metas;
    }

    @Override
    public Optional<ChunkMetadata> latestChunkMeta(Path connectorSnapshotDir) throws IOException {
        List<ChunkMetadata> list = listChunkMetasOrdered(connectorSnapshotDir);
        return list.isEmpty() ? Optional.empty() : Optional.of(list.get(list.size() - 1));
    }

    @Override
    public OptionalSnapshotChunk loadLatestChunk(Path connectorSnapshotDir) throws IOException {
        Optional<ChunkMetadata> meta = latestChunkMeta(connectorSnapshotDir);
        if (meta.isEmpty()) {
            return new OptionalSnapshotChunk(null, null);
        }
        ChunkMetadata m = meta.get();
        String prefix = ChunkNaming.prefix(m.startRow(), m.endRow());
        List<EngineRow> rows = readChunkData(connectorSnapshotDir, prefix);
        return new OptionalSnapshotChunk(m, rows);
    }

    @Override
    public void writeChunk(
            Path connectorSnapshotDir,
            int startRow,
            int endRow,
            List<EngineRow> rows,
            ChunkMetadata meta)
            throws IOException {
        Files.createDirectories(connectorSnapshotDir);
        String prefix = ChunkNaming.prefix(startRow, endRow);
        Path dataPath = connectorSnapshotDir.resolve(ChunkNaming.dataFile(prefix));
        Path metaPath = connectorSnapshotDir.resolve(ChunkNaming.metaFile(prefix));
        SnapshotJson.mapper().writeValue(dataPath.toFile(), new ChunkData(rows));
        SnapshotJson.mapper().writeValue(metaPath.toFile(), meta);
    }

    @Override
    public List<EngineRow> readChunkData(Path connectorSnapshotDir, String prefix) throws IOException {
        Path dataPath = connectorSnapshotDir.resolve(ChunkNaming.dataFile(prefix));
        ChunkData data = SnapshotJson.mapper().readValue(dataPath.toFile(), ChunkData.class);
        return data.rows();
    }

    @Override
    public void writeMaterializedSnapshot(
            Path materializedDir, List<EngineRow> rows, ChunkMetadata meta, String manifestJsonUtf8)
            throws IOException {
        Files.createDirectories(materializedDir);
        if (manifestJsonUtf8 != null && !manifestJsonUtf8.isBlank()) {
            Files.writeString(
                    materializedDir.resolve(MATERIALIZED_MANIFEST_FILE),
                    manifestJsonUtf8,
                    StandardCharsets.UTF_8);
        }
        int n = rows.size();
        if (meta == null || n == 0) {
            return;
        }
        int lastIdx = n - 1;
        String prefix = ChunkNaming.prefix(0, lastIdx);
        Path dataPath = materializedDir.resolve(ChunkNaming.dataFile(prefix));
        Path metaPath = materializedDir.resolve(ChunkNaming.metaFile(prefix));
        SnapshotJson.mapper().writeValue(dataPath.toFile(), new ChunkData(rows));
        SnapshotJson.mapper().writeValue(metaPath.toFile(), meta);
    }

    @Override
    public String readMaterializedManifest(Path materializedDir) throws IOException {
        Path m = materializedDir.resolve(MATERIALIZED_MANIFEST_FILE);
        if (!Files.exists(m)) {
            return null;
        }
        return Files.readString(m, StandardCharsets.UTF_8);
    }

    @Override
    public List<EngineRow> readMaterializedRows(Path materializedDir, String chunkPrefix) throws IOException {
        Path dataPath = materializedDir.resolve(ChunkNaming.dataFile(chunkPrefix));
        ChunkData data = SnapshotJson.mapper().readValue(dataPath.toFile(), ChunkData.class);
        return data.rows();
    }

    @Override
    public boolean exists(Path path) {
        return Files.exists(path);
    }
}
