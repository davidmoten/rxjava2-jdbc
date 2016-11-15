package org.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.github.davidmoten.guavamini.Lists;
import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Flowable;

public class TransactedBuilder {

    private final Flowable<Connection> connections;
    private SelectBuilder selectBuilder;

    public TransactedBuilder(TransactedConnection con) {
        this.connections = Flowable.just(con);
    }

    public TransactedSelectBuilder select(String sql) {
        this.selectBuilder = new SelectBuilder(sql, connections);
        return selectBuilder.transacted();
    }

}
