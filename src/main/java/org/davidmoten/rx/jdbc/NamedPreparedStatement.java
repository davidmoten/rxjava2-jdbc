package org.davidmoten.rx.jdbc;

import java.sql.PreparedStatement;
import java.util.List;

class NamedPreparedStatement {
    final PreparedStatement ps;
    final List<String> names;

    NamedPreparedStatement(PreparedStatement ps, List<String> names) {
        this.ps = ps;
        this.names = names;
    }
    

}