package org.davidmoten.rx.jdbc.pool;

import org.davidmoten.rx.pool.Consumers;
import org.junit.Test;

import com.github.davidmoten.junit.Asserts;

public class ConsumersTest {
    
    @Test
    public void isUtilityClass() {
        Asserts.assertIsUtilityClass(Consumers.class);
    }
    
    
    @Test
    public void testDoNothing() throws Exception {
        Consumers.doNothing().accept(1);
    }

}
