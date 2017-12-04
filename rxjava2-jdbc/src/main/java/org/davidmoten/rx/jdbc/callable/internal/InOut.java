package org.davidmoten.rx.jdbc.callable.internal;

import org.davidmoten.rx.jdbc.Type;

public final class InOut implements InParameterPlaceholder, OutParameterPlaceholder {
    final Type type;
    final Class<?> cls;

    public InOut(Type type, Class<?> cls) {
        this.type = type;
        this.cls = cls;
    }

    @Override
    public Type type() {
        return type;
    }
}
