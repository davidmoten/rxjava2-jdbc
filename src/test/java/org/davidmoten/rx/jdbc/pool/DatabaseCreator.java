package org.davidmoten.rx.jdbc.pool;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.davidmoten.rx.jdbc.ConnectionProvider;
import org.davidmoten.rx.jdbc.Database;
import org.davidmoten.rx.jdbc.exceptions.SQLRuntimeException;

public class DatabaseCreator {

    private static AtomicInteger dbNumber = new AtomicInteger();
    

    public static Database create(int maxSize) {
        return Database
                .from(new NonBlockingConnectionPool(connectionProvider(nextUrl()), maxSize, 1000));
    }

    public static ConnectionProvider connectionProvider() {
        return connectionProvider(nextUrl());
    }
    
    private static ConnectionProvider connectionProvider(String url) {
        return new ConnectionProvider() {

            private final AtomicBoolean once = new AtomicBoolean(false);

            @Override
            public Connection get() {
                try {
                    Connection c = DriverManager.getConnection(url);
                    synchronized (this) {
                        if (once.compareAndSet(false, true)) {
                            createDatabase(c);
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

    static Connection connection() {
        try {
            Connection c = DriverManager.getConnection(nextUrl());
            createDatabase(c);
            return c;
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }
    }

    private static String nextUrl() {
        return "jdbc:h2:mem:test" + dbNumber.incrementAndGet() + ";DB_CLOSE_DELAY=-1";
    }

    private static void createDatabase(Connection c) {
        try {
            c.setAutoCommit(true);
            c.prepareStatement(
                    "create table person (name varchar(50) primary key, score int not null,dob date, registered timestamp)")
                    .execute();
            c.prepareStatement("insert into person(name,score) values('FRED',21)").execute();
            c.prepareStatement("insert into person(name,score) values('JOSEPH',34)").execute();
            c.prepareStatement("insert into person(name,score) values('MARMADUKE',25)").execute();

            c.prepareStatement(
                    "create table person_clob (name varchar(50) not null,  document clob)")
                    .execute();
            c.prepareStatement(
                    "create table person_blob (name varchar(50) not null, document blob)")
                    .execute();
            c.prepareStatement(
                    "create table address (address_id int primary key, full_address varchar(255) not null)")
                    .execute();
            c.prepareStatement(
                    "insert into address(address_id, full_address) values(1,'57 Something St, El Barrio, Big Place')")
                    .execute();
            c.prepareStatement(
                    "create table note(id bigint auto_increment primary key, text varchar(255))")
                    .execute();
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }
    }

}
