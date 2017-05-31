package org.davidmoten.rx.jdbc.pool;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.IOUtils;
import org.davidmoten.rx.jdbc.ConnectionProvider;
import org.davidmoten.rx.jdbc.Database;
import org.davidmoten.rx.jdbc.exceptions.SQLRuntimeException;

public class DatabaseCreator {

    private static AtomicInteger dbNumber = new AtomicInteger();

    public static Database create(int maxSize) {
        return create(maxSize, false);
    }

    public static Database createDerby(int maxSize) {
        return Database.from(
                Pools.nonBlocking().connectionProvider(connectionProviderDerby(nextUrlDerby()))
                        .maxPoolSize(maxSize).build());
    }

    private static ConnectionProvider connectionProviderDerby(String url) {
        return new ConnectionProvider() {

            private final AtomicBoolean once = new AtomicBoolean(false);

            @Override
            public Connection get() {
                try {
                    Connection c = DriverManager.getConnection(url);
                    synchronized (this) {
                        if (once.compareAndSet(false, true)) {
                            createDatabaseDerby(c);
                        }
                    }
                    return c;
                } catch (SQLException e) {
                    throw new SQLRuntimeException(e);
                }
            }

            @Override
            public void close() {
                //
            }
        };
    }

    private static void createDatabaseDerby(Connection c) throws SQLException {
        c.setAutoCommit(true);
        exec(c, "create table note2("
                + "id integer not null generated always as identity (start with 1, increment by 2),"
                + "text varchar(255) not null," + "constraint primary_key primary key (id)"
                + ")");
    }

    public static Database create(int maxSize, boolean big) {
        return Database
                .from(Pools.nonBlocking().connectionProvider(connectionProvider(nextUrl(), big))
                        .maxPoolSize(maxSize).build());
    }

    public static ConnectionProvider connectionProvider() {
        return connectionProvider(nextUrl(), false);
    }

    private static ConnectionProvider connectionProvider(String url, boolean big) {
        return new ConnectionProvider() {

            private final AtomicBoolean once = new AtomicBoolean(false);

            @Override
            public Connection get() {
                try {
                    Connection c = DriverManager.getConnection(url);
                    synchronized (this) {
                        if (once.compareAndSet(false, true)) {
                            createDatabase(c, big);
                        }
                    }
                    return c;
                } catch (SQLException e) {
                    throw new SQLRuntimeException(e);
                }
            }

            @Override
            public void close() {
                //
            }
        };
    }

    private static String nextUrl() {
        return "jdbc:h2:mem:test" + dbNumber.incrementAndGet() + ";DB_CLOSE_DELAY=-1";
    }

    private static String nextUrlDerby() {
        return "jdbc:derby:memory:derbyUnitTest" + dbNumber.incrementAndGet() + ";create=true";
    }

    private static void createDatabase(Connection c, boolean big) {
        try {
            c.setAutoCommit(true);
            c.prepareStatement(
                    "create table person (name varchar(50) primary key, score int not null,dob date, registered timestamp)")
                    .execute();
            if (big) {
                List<String> lines = IOUtils.readLines(
                        DatabaseCreator.class.getResourceAsStream("/big.txt"),
                        StandardCharsets.UTF_8);
                lines.stream().map(line -> line.split("\t")).forEach(items -> {
                    try {
                        c.prepareStatement("insert into person(name,score) values('" + items[0]
                                + "'," + Integer.parseInt(items[1]) + ")").execute();
                    } catch (SQLException e) {
                        throw new SQLRuntimeException(e);
                    }
                });
            } else {
                exec(c, "insert into person(name,score) values('FRED',21)");
                exec(c, "insert into person(name,score) values('JOSEPH',34)");
                exec(c, "insert into person(name,score) values('MARMADUKE',25)");
            }

            exec(c, "create table person_clob (name varchar(50) not null,  document clob)");
            exec(c, "create table person_blob (name varchar(50) not null, document blob)");

            exec(c, "create table address (address_id int primary key, full_address varchar(255) not null)");
            exec(c, "insert into address(address_id, full_address) values(1,'57 Something St, El Barrio, Big Place')");
            exec(c, "create table note(id bigint auto_increment primary key, text varchar(255))");
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void exec(Connection c, String sql) throws SQLException {
        c.prepareStatement(sql).execute();
    }

}
