package org.davidmoten.rx.jdbc;

import static org.junit.Assert.assertEquals;

import java.util.Collections;

import org.junit.Test;

import com.github.davidmoten.guavamini.Lists;

public class SqlInfoTest {

    @Test
    public void testExpansionNoParameters() {
        SqlInfo s = SqlInfo.parse("? ?", Collections.emptyList());
        assertEquals("? ?", s.sql());
    }

    @Test
    public void testExpansionEmpty() {
        SqlInfo s = SqlInfo.parse("", Collections.emptyList());
        assertEquals("", s.sql());
    }

    @Test
    public void testExpansionWithParametersNoCollections() {
        Parameter p = new Parameter(12);
        SqlInfo s = SqlInfo.parse("? hello ?", Lists.newArrayList(p, p));
        assertEquals("? hello ?", s.sql());
    }

    @Test
    public void testExpansionWithParametersCollectionPresent() {
        Parameter p1 = new Parameter(12);
        Parameter p2 = new Parameter(Lists.newArrayList(1, 2, 3));
        SqlInfo s = SqlInfo.parse("? hello ?", Lists.newArrayList(p1, p2));
        assertEquals("? hello ?,?,?", s.sql());
    }

}
