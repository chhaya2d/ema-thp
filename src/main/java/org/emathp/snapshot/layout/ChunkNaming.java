package org.emathp.snapshot.layout;

/** Filename prefix {@code %06d_%06d} for inclusive logical row range. */
public final class ChunkNaming {

    private ChunkNaming() {}

    public static String prefix(int startRowInclusive, int endRowInclusive) {
        return String.format("%06d_%06d", startRowInclusive, endRowInclusive);
    }

    public static String dataFile(String prefix) {
        return prefix + "_data.json";
    }

    public static String metaFile(String prefix) {
        return prefix + "_meta.json";
    }

    /** Parses {@code 000000_000005} from a filename prefix segment. */
    public static int[] parsePrefix(String fileName) {
        String base = fileName.contains("_") ? fileName.substring(0, fileName.lastIndexOf('_')) : fileName;
        int idx = base.indexOf('_');
        if (idx <= 0 || idx == base.length() - 1) {
            throw new IllegalArgumentException("Invalid chunk file: " + fileName);
        }
        String a = base.substring(0, idx);
        String b = base.substring(idx + 1);
        return new int[] {Integer.parseInt(a), Integer.parseInt(b)};
    }
}
