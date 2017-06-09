package org.davidmoten.rx.jdbc;

import java.sql.PreparedStatement;
import java.util.List;

import javax.annotation.Nonnull;

final class NamedPreparedStatement {
    final PreparedStatement ps;
    final List<String> names;

    NamedPreparedStatement(@Nonnull PreparedStatement ps, @Nonnull List<String> names) {
        this.ps = ps;
        this.names = names;
    }

}