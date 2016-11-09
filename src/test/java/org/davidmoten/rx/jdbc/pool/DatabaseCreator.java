package org.davidmoten.rx.jdbc.pool;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

import org.davidmoten.rx.jdbc.ConnectionProvider;
import org.davidmoten.rx.jdbc.Database;
import org.davidmoten.rx.jdbc.SQLRuntimeException;

public class DatabaseCreator {

    private static AtomicInteger dbNumber = new AtomicInteger();

    public static String nextUrl() {
        return "jdbc:h2:mem:test" + dbNumber.incrementAndGet() + ";DB_CLOSE_DELAY=-1";
    }

    public static Database create() {
        return Database.from(nextConnection());
    }

    public static Database createDatabase(ConnectionProvider cp) {
        Database db = Database.from(cp);
        Connection con = cp.get();
        createDatabase(con);
        try {
            con.close();
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }
        return db;
    }

    static Connection nextConnection() {
        try {
            Connection con = DriverManager.getConnection(DatabaseCreator.nextUrl());
            createDatabase(con);
            return con;
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }
    }

    public static void createDatabase(Connection c) {
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
