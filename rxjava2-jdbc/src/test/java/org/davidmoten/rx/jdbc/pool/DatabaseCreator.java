package org.davidmoten.rx.jdbc.pool;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.IOUtils;
import org.davidmoten.rx.jdbc.ConnectionProvider;
import org.davidmoten.rx.jdbc.Database;
import org.davidmoten.rx.jdbc.exceptions.SQLRuntimeException;

import io.reactivex.Scheduler;
import io.reactivex.schedulers.Schedulers;

public final class DatabaseCreator {

    private static final AtomicInteger dbNumber = new AtomicInteger();

    public static Database createBlocking() {
        return Database.fromBlocking(connectionProvider());
    }

    public static Database create(int maxSize) {
        ExecutorService executor = Executors.newFixedThreadPool(maxSize);
        Scheduler scheduler = Schedulers.from(executor);
        return create(maxSize, false, scheduler);
    }

    public static Database create(int maxSize, Scheduler scheduler) {
        return create(maxSize, false, scheduler);
    }

    public static Database createDerby(int maxSize) {
        return createDerby(maxSize, false);
    }

    public static Database createDerbyWithStoredProcs(int maxSize) {
        return createDerby(maxSize, true);
    }

    private static Database createDerby(int maxSize, boolean withStoredProcs) {
        return Database.from(Pools.nonBlocking() //
                .connectionProvider(connectionProviderDerby(nextUrlDerby(), withStoredProcs)) //
                .maxPoolSize(maxSize) //
                .scheduler(Schedulers.from(Executors.newFixedThreadPool(maxSize))) //
                .build());
    }

    private static ConnectionProvider connectionProviderDerby(String url, boolean withStoredProcs) {
        Connection c;
        try {
            c = DriverManager.getConnection(url);
            createDatabaseDerby(c);
            if (withStoredProcs) {
                addStoredProcs(c);
            }
        } catch (SQLException e) {
            throw new SQLRuntimeException(e);
        }
        return new ConnectionProvider() {

            @Override
            public Connection get() {
                try {
                    return DriverManager.getConnection(url);
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
                + "text varchar(255) not null," //
                + "constraint primary_key primary key (id)" + ")");
        exec(c, "create table app.person (name varchar(50) primary key, score int not null)");
        exec(c, "insert into app.person(name, score) values('FRED', 24)");
        exec(c, "insert into app.person(name, score) values('SARAH', 26)");
    }

    private static void addStoredProcs(Connection c) throws SQLException {
        exec(c, "call sqlj.install_jar('target/rxjava2-jdbc-stored-procedure.jar', 'APP.examples', 0)");

        {
            String sql = "CREATE PROCEDURE APP.zero()" //
                    + " PARAMETER STYLE JAVA" //
                    + " LANGUAGE JAVA" //
                    + " EXTERNAL NAME" //
                    + " 'org.davidmoten.rx.jdbc.StoredProcExample.zero'";
            exec(c, sql);
        }
        {
            String sql = "CREATE PROCEDURE APP.inout1" //
                    + " (INOUT a INTEGER)" //
                    + " PARAMETER STYLE JAVA" //
                    + " LANGUAGE JAVA" //
                    + " EXTERNAL NAME" //
                    + " 'org.davidmoten.rx.jdbc.StoredProcExample.inout1'";
            exec(c, sql);
        }
        {
            String sql = "CREATE PROCEDURE APP.inout2" //
                    + " (INOUT a INTEGER, " //
                    + " INOUT b INTEGER)" //
                    + " PARAMETER STYLE JAVA" //
                    + " LANGUAGE JAVA" //
                    + " EXTERNAL NAME" //
                    + " 'org.davidmoten.rx.jdbc.StoredProcExample.inout2'";
            exec(c, sql);
        }

        {
            String sql = "CREATE PROCEDURE APP.inout3" //
                    + " (INOUT a INTEGER, " //
                    + " INOUT b INTEGER, INOUT c INTEGER)" //
                    + " PARAMETER STYLE JAVA" //
                    + " LANGUAGE JAVA" //
                    + " EXTERNAL NAME" //
                    + " 'org.davidmoten.rx.jdbc.StoredProcExample.inout3'";
            exec(c, sql);
        }
        {
            String sql = "CREATE PROCEDURE APP.in1out1" //
                    + " (IN a INTEGER," //
                    + " OUT b INTEGER)" //
                    + " PARAMETER STYLE JAVA" //
                    + " LANGUAGE JAVA" //
                    + " EXTERNAL NAME" //
                    + " 'org.davidmoten.rx.jdbc.StoredProcExample.in1out1'";
            exec(c, sql);
        }

        {
            String sql = "CREATE PROCEDURE APP.in1out2" //
                    + " (IN a INTEGER," //
                    + " OUT b INTEGER," //
                    + " OUT c INTEGER)" //
                    + " PARAMETER STYLE JAVA" //
                    + " LANGUAGE JAVA" //
                    + " EXTERNAL NAME" //
                    + " 'org.davidmoten.rx.jdbc.StoredProcExample.in1out2'";
            exec(c, sql);
        }

        {
            String sql = "CREATE PROCEDURE APP.in1out3" //
                    + " (IN a INTEGER," //
                    + " OUT b INTEGER," //
                    + " OUT c INTEGER," //
                    + " OUT d INTEGER)" //
                    + " PARAMETER STYLE JAVA" //
                    + " LANGUAGE JAVA" //
                    + " EXTERNAL NAME" //
                    + " 'org.davidmoten.rx.jdbc.StoredProcExample.in1out3'";
            exec(c, sql);
        }
        {
            String sql = "CREATE PROCEDURE APP.out10" //
                    + " (OUT a INTEGER," //
                    + " OUT b INTEGER," //
                    + " OUT c INTEGER," //
                    + " OUT d INTEGER," //
                    + " OUT e INTEGER," //
                    + " OUT f INTEGER," //
                    + " OUT g INTEGER," //
                    + " OUT h INTEGER," //
                    + " OUT i INTEGER," //
                    + " OUT j INTEGER)" //
                    + " PARAMETER STYLE JAVA" //
                    + " LANGUAGE JAVA" //
                    + " EXTERNAL NAME" //
                    + " 'org.davidmoten.rx.jdbc.StoredProcExample.out10'";
            exec(c, sql);
        }

        {
            String sql = "CREATE PROCEDURE APP.in0out0rs1" //
                    + " ()" //
                    + " PARAMETER STYLE JAVA" //
                    + " LANGUAGE JAVA" //
                    + " READS SQL DATA" //
                    + " DYNAMIC RESULT SETS 1" //
                    + " EXTERNAL NAME" //
                    + " 'org.davidmoten.rx.jdbc.StoredProcExample.in0out0rs1'";
            exec(c, sql);
        }
        {
            String sql = "CREATE PROCEDURE APP.rs10" //
                    + " ()" //
                    + " PARAMETER STYLE JAVA" //
                    + " LANGUAGE JAVA" //
                    + " READS SQL DATA" //
                    + " DYNAMIC RESULT SETS 10" //
                    + " EXTERNAL NAME" //
                    + " 'org.davidmoten.rx.jdbc.StoredProcExample.rs10'";
            exec(c, sql);
        }
        {
            String sql = "CREATE PROCEDURE APP.GETPERSONCOUNT" //
                    + " (IN MIN_SCORE INTEGER," //
                    + " OUT COUNT INTEGER)" //
                    + " PARAMETER STYLE JAVA" //
                    + " LANGUAGE JAVA" //
                    + " EXTERNAL NAME" //
                    + " 'org.davidmoten.rx.jdbc.StoredProcExample.getPersonCount'";
            exec(c, sql);
        }

        {
            String sql = "CREATE PROCEDURE APP.in1out0rs2(in min_score integer)" //
                    + " PARAMETER STYLE JAVA" //
                    + " LANGUAGE JAVA" //
                    + " READS SQL DATA" //
                    + " DYNAMIC RESULT SETS 2" //
                    + " EXTERNAL NAME" //
                    + " 'org.davidmoten.rx.jdbc.StoredProcExample.in1out0rs2'";
            exec(c, sql);
        }

        {
            String sql = "CREATE PROCEDURE APP.in0out2rs3(OUT a INTEGER, OUT b INTEGER)" //
                    + " PARAMETER STYLE JAVA" //
                    + " LANGUAGE JAVA" //
                    + " READS SQL DATA" //
                    + " DYNAMIC RESULT SETS 3" //
                    + " EXTERNAL NAME" //
                    + " 'org.davidmoten.rx.jdbc.StoredProcExample.in0out2rs3'";
            exec(c, sql);
        }

        exec(c, "CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" + "'derby.database.classpath', 'APP.examples')");
    }

    public static Database create(int maxSize, boolean big, Scheduler scheduler) {
        NonBlockingConnectionPool pool = Pools.nonBlocking() //
                .connectionProvider(connectionProvider(nextUrl(), big)) //
                .maxPoolSize(maxSize) //
                .scheduler(scheduler) //
                .build();
        return Database.from(pool, () -> {
            pool.close();
            scheduler.shutdown();
        });
    }

    public static ConnectionProvider connectionProvider() {
        return connectionProvider(nextUrl(), false);
    }

    public static ConnectionProvider connectionProviderDerby() {
        return connectionProvider(nextUrlDerby(), false);
    }

    private static ConnectionProvider connectionProvider(String url, boolean big) {
        return new ConnectionProvider() {

            private final AtomicBoolean once = new AtomicBoolean(false);
            private final CountDownLatch latch = new CountDownLatch(1);

            @Override
            public Connection get() {
                try {
                    if (once.compareAndSet(false, true)) {
                        try {
                            Connection c = DriverManager.getConnection(url);
                            createDatabase(c, big);
                            c.setAutoCommit(false);
                            return c;
                        } finally {
                            latch.countDown();
                        }
                    } else {
                        if (latch.await(30, TimeUnit.SECONDS)) {
                            return DriverManager.getConnection(url);
                        } else {
                            throw new TimeoutException("big database timed out on creation");
                        }
                    }
                } catch (SQLException | InterruptedException | TimeoutException e) {
                    throw new SQLRuntimeException(e);
                }
            }

            @Override
            public void close() {
                //
            }
        };
    }

    public static String nextUrl() {
        return "jdbc:h2:mem:test" + dbNumber.incrementAndGet() + ";DB_CLOSE_DELAY=-1";
    }

    private static String nextUrlDerby() {
        return "jdbc:derby:memory:derbyUnitTest" + dbNumber.incrementAndGet() + ";create=true";
    }

    private static void createDatabase(Connection c, boolean big) {
        try {
            c.setAutoCommit(true);
            c.prepareStatement(
                    "create table person (name varchar(50) primary key, score int not null, date_of_birth date, registered timestamp)")
                    .execute();
            if (big) {
                List<String> lines = IOUtils.readLines(DatabaseCreator.class.getResourceAsStream("/big.txt"),
                        StandardCharsets.UTF_8);
                lines.stream().map(line -> line.split("\t")).forEach(items -> {
                    try {
                        c.prepareStatement("insert into person(name,score) values('" + items[0] + "',"
                                + Integer.parseInt(items[1]) + ")").execute();
                    } catch (SQLException e) {
                        throw new SQLRuntimeException(e);
                    }
                });
            } else {
                exec(c, "insert into person(name,score,registered) values('FRED',21, {ts '2015-09-17 18:47:52.69Z'})");
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
        } 
    }

    private static void exec(Connection c, String sql) throws SQLException {
        c.prepareStatement(sql).execute();
    }

}
