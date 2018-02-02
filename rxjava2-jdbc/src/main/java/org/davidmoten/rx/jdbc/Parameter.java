package org.davidmoten.rx.jdbc;

import java.util.Collection;
import java.util.List;

import com.github.davidmoten.guavamini.Lists;

/**
 * Encapsulates a query parameter.
 */
public final class Parameter {
    
    static final Parameter NULL = new Parameter(null);

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

    boolean isCollection() {
        return value != null && (value instanceof Collection);
    }

    int size() {
        // TODO cache all these calcs when constructed
        if (value != null) {
            if (value instanceof Collection) {
                return ((Collection<?>) value).size();
            } else {
                return 1;
            }
        } else {
            return 1;
        }
    }

    boolean hasName() {
        return name != null;
    }

    String name() {
        return name;
    }

    public static ParameterListBuilder named(String name, String value) {
        return new ParameterListBuilder(Lists.newArrayList()).named(name, value);
    }

    public static class ParameterListBuilder {
        private final List<Parameter> list;
        private String lastName;

        ParameterListBuilder(List<Parameter> list) {
            this.list = list;
        }

        public ParameterListBuilder named(String name, String value) {
            list.add(new Parameter(name, value));
            lastName = name;
            return this;
        }

        public ParameterListBuilder value(String value) {
            return named(lastName, value);
        }

        public List<Parameter> list() {
            return list;
        }
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("Parameter[");
        b.append("name=");
        b.append(name);
        b.append(", value=");
        b.append(String.valueOf(value));
        b.append("]");
        return b.toString();
    }

    public static Parameter create(String name, Object value) {
        return new Parameter(name, value);
    }

}
