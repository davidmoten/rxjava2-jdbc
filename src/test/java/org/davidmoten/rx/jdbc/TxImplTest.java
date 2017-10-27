package org.davidmoten.rx.jdbc;

import static org.junit.Assert.assertTrue;

import org.junit.Ignore;
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

    @Test
    public void testToStringCompletes() {
        String s = Database.test() //
                .select("select score from person order by score") //
                .transacted() //
                .getAs(Integer.class) //
                .lastOrError() //
                .map(tx -> tx.toString()) //
                .blockingGet();
        assertTrue(s.startsWith("TxImpl ["));
    }

    @Test
    @Ignore
    // TODO error throwing right for transactions?
    public void testToStringForError() {
        String s = Database.test() //
                .select("select scorezzz from person order by score") //
                .transacted() //
                .getAs(Integer.class) //
                .lastOrError() //
                .map(tx -> tx.toString()) //
                .blockingGet();
        assertTrue(s.startsWith("TxImpl ["));
    }

}
