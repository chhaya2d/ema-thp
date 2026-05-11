package org.emathp.snapshot.model;

import java.util.List;
import org.emathp.model.EngineRow;

/** Serialized chunk payload: engine rows for inclusive [{@code startRow},{@code endRow}]. */
public record ChunkData(List<EngineRow> rows) {}
