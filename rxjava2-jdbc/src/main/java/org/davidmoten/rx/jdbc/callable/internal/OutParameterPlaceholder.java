package org.davidmoten.rx.jdbc.callable.internal;

import org.davidmoten.rx.jdbc.Type;

public interface OutParameterPlaceholder extends ParameterPlaceholder {
    Type type();
}
