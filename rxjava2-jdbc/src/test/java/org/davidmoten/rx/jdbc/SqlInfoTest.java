package org.davidmoten.rx.jdbc;

import static org.junit.Assert.assertEquals;

import java.util.Collections;

import org.junit.Test;

import com.github.davidmoten.guavamini.Lists;

public class SqlInfoTest {

    @Test
    public void testExpansionNoParameters() {
        String s = SqlInfo.expandQuestionMarks("? ?", Collections.emptyList());
        assertEquals("? ?", s);
    }

    @Test
    public void testExpansionEmpty() {
        String s = SqlInfo.expandQuestionMarks("", Collections.emptyList());
        assertEquals("", s);
    }

    @Test
    public void testExpansionWithParametersNoCollections() {
        Parameter p = new Parameter(12);
        String s = SqlInfo.expandQuestionMarks("? hello ?", Lists.newArrayList(p, p));
        assertEquals("? hello ?", s);
    }

    @Test
    public void testExpansionWithParametersCollectionPresent() {
        Parameter p1 = new Parameter(12);
        Parameter p2 = new Parameter(Lists.newArrayList(1, 2, 3));
        String s = SqlInfo.expandQuestionMarks("? hello ?", Lists.newArrayList(p1, p2));
        assertEquals("? hello ?,?,?", s);
    }

}
