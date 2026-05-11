package org.emathp.model;

/**
 * Marker for the result of {@link org.emathp.parser.SQLParserService#parse(String)}.
 *
 * <p>The parser produces one of two shapes today:
 * <ul>
 *   <li>{@link Query} - a single-source query (SELECT/WHERE/ORDER BY/LIMIT against one logical
 *       source, dispatched per-connector by the planner).</li>
 *   <li>{@link JoinQuery} - a two-source equi-join, joined engine-side. See ADR-0004 (TBD).</li>
 * </ul>
 *
 * @apiNote Sealed so callers can pattern-match exhaustively; every consumer is forced to decide
 *          how to handle each shape rather than silently treating a JOIN as a single-source.
 */
public sealed interface ParsedQuery permits Query, JoinQuery {}
