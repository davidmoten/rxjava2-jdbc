package org.davidmoten.rx.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

public class LifoQueueTest {
    
    @Test
    public void testIsLifo() {
        LifoQueue<Integer> q = new LifoQueue<>();
        q.offer(1);
        q.offer(2);
        assertEquals(2, (int) q.poll());
        assertEquals(1, (int) q.poll());
        assertNull(q.poll());
    }

}
