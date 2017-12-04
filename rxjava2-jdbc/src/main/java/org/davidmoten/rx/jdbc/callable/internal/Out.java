package org.davidmoten.rx.jdbc.callable.internal;

import org.davidmoten.rx.jdbc.Type;

public final class Out implements OutParameterPlaceholder {
    final Type type;
    final Class<?> cls;

    public Out(Type type, Class<?> cls) {
        this.type = type;
        this.cls = cls;
    }

    @Override
    public Type type() {
        return type;
    }
}