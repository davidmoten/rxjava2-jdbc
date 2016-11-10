package org.davidmoten.rx.jdbc.pool;

import org.davidmoten.rx.jdbc.Database;
import org.junit.Test;

public class DatabaseTest {

    @Test
    public void test() {
        Database db = DatabaseCreator.create(5);
        db.select("select score from person where name=?") //
                .parameters("fred", "john");
    }

}
