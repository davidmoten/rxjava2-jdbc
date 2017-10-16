package org.davidmoten.rx.jdbc;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class ParameterTest {

    @Test
    public void testToString() {
        assertEquals("Parameter[name=location, value=there]",
                Parameter.create("location", "there").toString());
    }

}
