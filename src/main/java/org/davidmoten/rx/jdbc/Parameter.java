package org.davidmoten.rx.jdbc;

/**
 * Encapsulates a query parameter.
 */
final class Parameter {

    private final String name;
    /**
     * Actual query parameter value to be encapsulated.
     */
    private final Object value;

    /**
     * Constructor.
     * 
     * @param parameter
     */
    Parameter(Object value) {
        this(null, value);
    }

    Parameter(String name, Object value) {
        this.name = name;
        this.value = value;
    }

    /**
     * Returns the parameter value.
     * 
     * @return
     */
    Object value() {
        return value;
    }

    boolean hasName() {
        return name != null;
    }

    String name() {
        return name;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        if (value instanceof String)
            builder.append("'");
        builder.append(value);
        if (value instanceof String)
            builder.append("'");
        return builder.toString();
    }

}
