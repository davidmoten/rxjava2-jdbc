package org.davidmoten.rx.jdbc;

import java.sql.Connection;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

class TxImpl<T> implements Tx<T> {

        private final Connection con;
        private final T value;
        private final Throwable e;
        private final boolean completed;
        private final AtomicInteger counter;
        private final AtomicBoolean once = new AtomicBoolean(false);

        public TxImpl(Connection con, T value, Throwable e, boolean completed,
                AtomicInteger counter) {
            this.con = con;
            this.value = value;
            this.e = e;
            this.completed = completed;
            this.counter = counter;
        }

        @Override
        public boolean isValue() {
            return !completed && e != null;
        }

        @Override
        public boolean isComplete() {
            return completed;
        }

        @Override
        public boolean isError() {
            return e != null;
        }

        @Override
        public T value() {
            return value;
        }

        @Override
        public Throwable throwable() {
            return e;
        }

        public Connection connection() {
            return con;
        }

    }
