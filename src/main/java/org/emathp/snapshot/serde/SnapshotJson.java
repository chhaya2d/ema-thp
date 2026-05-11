package org.emathp.snapshot.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/** Jackson mapper for snapshot JSON files only (human-readable, ISO-8601 instants). */
public final class SnapshotJson {

    private static final ObjectMapper MAPPER = create();

    private SnapshotJson() {}

    public static ObjectMapper mapper() {
        return MAPPER;
    }

    private static ObjectMapper create() {
        ObjectMapper om = new ObjectMapper();
        om.registerModule(new JavaTimeModule());
        om.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        om.findAndRegisterModules();
        return om;
    }
}
