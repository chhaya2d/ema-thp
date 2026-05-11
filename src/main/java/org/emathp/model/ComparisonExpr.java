package org.emathp.model;

public record ComparisonExpr(String field, Operator operator, Object value) {}
