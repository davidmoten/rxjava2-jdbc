package org.davidmoten.rx.jdbc;

import java.sql.CallableStatement;
import java.util.List;

import javax.annotation.Nonnull;

public class NamedCallableStatement {
    final CallableStatement stmt;
    final List<String> names;

    NamedCallableStatement(@Nonnull CallableStatement stmt, @Nonnull List<String> names) {
        this.stmt = stmt;
        this.names = names;
    }
}
