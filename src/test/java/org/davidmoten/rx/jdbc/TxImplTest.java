package org.davidmoten.rx.jdbc;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class TxImplTest {

    @Test
    public void testToString() {
        String s = Database.test().select("select score from person order by score") //
                .transactedValuesOnly() //
                .getAs(Integer.class) //
                .map(tx -> tx.toString()) //
                .firstOrError() //
                .blockingGet();
        assertTrue(s.startsWith("TxImpl ["));
    }

}
