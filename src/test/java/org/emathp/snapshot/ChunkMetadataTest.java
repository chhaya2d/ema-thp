package org.emathp.snapshot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import org.emathp.snapshot.adapters.fs.FsSnapshotStore;
import org.emathp.snapshot.model.ChunkMetadata;
import org.emathp.snapshot.serde.SnapshotJson;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ChunkMetadataTest {

    @Test
    void latestChunkIsMaxEndRow(@TempDir Path root) throws Exception {
        FsSnapshotStore storage = new FsSnapshotStore(root);
        Path dir = root.resolve("google-drive");
        Files.createDirectories(dir);
        ChunkMetadata a =
                new ChunkMetadata(
                        0,
                        5,
                        Instant.now().toString(),
                        Instant.now().toString(),
                        "cursor_6",
                        false,
                        6);
        ChunkMetadata b =
                new ChunkMetadata(
                        6,
                        11,
                        Instant.now().toString(),
                        Instant.now().toString(),
                        "cursor_12",
                        false,
                        6);
        SnapshotJson.mapper().writeValue(dir.resolve("000000_000005_meta.json").toFile(), a);
        SnapshotJson.mapper().writeValue(dir.resolve("000006_000011_meta.json").toFile(), b);

        List<ChunkMetadata> ordered = storage.listChunkMetasOrdered(dir);
        assertEquals(2, ordered.size());
        ChunkMetadata latest = ordered.get(ordered.size() - 1);
        assertEquals(11, latest.endRow());
        assertEquals("cursor_12", latest.nextCursor());
        assertTrue(storage.latestChunkMeta(dir).isPresent());
        assertEquals(11, storage.latestChunkMeta(dir).get().endRow());
    }
}
