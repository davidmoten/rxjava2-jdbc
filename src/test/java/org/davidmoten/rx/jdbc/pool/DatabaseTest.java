package org.davidmoten.rx.jdbc.pool;

import org.davidmoten.rx.jdbc.Database;
import org.junit.Test;

public class DatabaseTest {

    @Test
    public void testSelect() {
        Database db = DatabaseCreator.create(1);
        db.select("select score from person where name=?") //
                .parameters("FRED", "JOSEPH") //
                .getAs(Integer.class) //
                .test() //
                .assertValues(21, 34) //
                .assertComplete();
    }

}
