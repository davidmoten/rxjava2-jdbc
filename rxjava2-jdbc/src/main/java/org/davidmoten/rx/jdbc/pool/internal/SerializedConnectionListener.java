package org.davidmoten.rx.jdbc.pool.internal;

import java.util.concurrent.atomic.AtomicInteger;

import org.davidmoten.rx.jdbc.pool.ConnectionListener;

import io.reactivex.internal.fuseable.SimplePlainQueue;
import io.reactivex.internal.queue.MpscLinkedQueue;

@SuppressWarnings("serial")
public final class SerializedConnectionListener extends AtomicInteger implements ConnectionListener {

    private final ConnectionListener c;
    private final SimplePlainQueue<Object> queue = new MpscLinkedQueue<Object>();

    public SerializedConnectionListener(ConnectionListener c) {
        this.c = c;
    }

    @Override
    public void onSuccess() {
        queue.offer(Boolean.TRUE);
        drain();
    }

    @Override
    public void onError(Throwable error) {
        queue.offer(error);
        drain();
    }

    private void drain() {
        if (getAndIncrement() == 0) {
            int missed = 1;
            while (true) {
                Object o;
                while ((o = queue.poll()) != null) {
                    if (o == Boolean.TRUE) {
                        c.onSuccess();
                    } else {
                        c.onError((Throwable) o);
                    }
                }
                missed = addAndGet(-missed);
                if (missed == 0) {
                    return;
                }
            }
        }
    }

}
