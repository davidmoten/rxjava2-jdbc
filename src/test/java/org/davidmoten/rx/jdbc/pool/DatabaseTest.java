package org.davidmoten.rx.jdbc.pool;

import org.davidmoten.rx.jdbc.Database;
import org.junit.Test;

public class DatabaseTest {

    private static Database db() {
        return DatabaseCreator.create(1);
    }

    @Test
    public void testSelectUsingQuestionMark() {
        db().select("select score from person where name=?") //
                .parameters("FRED", "JOSEPH") //
                .getAs(Integer.class) //
                .test() //
                .assertValues(21, 34) //
                .assertComplete();
    }

    @Test
    public void testSelectUsingName() {
        db().select("select score from person where name=:name") //
                .parameter("name", "FRED") //
                .parameter("name", "JOSEPH") //
                .getAs(Integer.class) //
                .test() //
                .assertValues(21, 34) //
                .assertComplete();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSelectUsingNameWithoutSpecifyingName() {
        db().select("select score from person where name=:name") //
                .parameters("FRED", "JOSEPH");
    }

}
