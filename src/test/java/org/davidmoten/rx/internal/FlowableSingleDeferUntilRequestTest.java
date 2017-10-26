package org.davidmoten.rx.internal;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import io.reactivex.Single;
import io.reactivex.subscribers.TestSubscriber;

public class FlowableSingleDeferUntilRequestTest {

    @Test
    public void testIsDeferred() {
        AtomicBoolean a = new AtomicBoolean();
        Single<Integer> s = Single.fromCallable(() -> {
            a.set(true);
            return 1;
        });
        TestSubscriber<Integer> ts = new FlowableSingleDeferUntilRequest<Integer>(s) //
                .test(0);
        assertFalse(a.get());
        ts.requestMore(1);
        assertTrue(a.get());
        ts.assertValue(1);
        ts.assertComplete();
    }

    @Test
    public void testErrorDeferred() {
        Single<Integer> s = Single.fromCallable(() -> {
            throw new RuntimeException("boo");
        });
        TestSubscriber<Integer> ts = new FlowableSingleDeferUntilRequest<Integer>(s) //
                .test();
        ts.assertError(RuntimeException.class);
        ts.assertNoValues();
    }

    @Test
    public void testCancelBeforeRequest() {
        Single<Integer> s = Single.fromCallable(() -> {
            return 1;
        });
        TestSubscriber<Integer> ts = new FlowableSingleDeferUntilRequest<Integer>(s) //
                .test(0);
        ts.cancel();
        ts.assertNoValues();
        ts.assertNotTerminated();
        ts.cancel();
    }

}
