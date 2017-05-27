package org.davidmoten.rx.jdbc;

import org.davidmoten.rx.jdbc.Select;
import org.junit.Test;

import com.github.davidmoten.junit.Asserts;

public class SelectTest {

    @Test
    public void isUtilityClass() {
        Asserts.assertIsUtilityClass(Select.class);
    }

}
