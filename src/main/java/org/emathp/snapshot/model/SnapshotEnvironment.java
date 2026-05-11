package org.emathp.snapshot.model;

/** Filesystem root selector under {@code data/}. */
public enum SnapshotEnvironment {
    PROD("prod"),
    TEST("test");

    private final String dirName;

    SnapshotEnvironment(String dirName) {
        this.dirName = dirName;
    }

    public String dirName() {
        return dirName;
    }
}
