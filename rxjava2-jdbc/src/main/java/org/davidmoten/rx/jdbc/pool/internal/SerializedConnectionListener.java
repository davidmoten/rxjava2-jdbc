package org.davidmoten.rx.jdbc.pool.internal;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import io.reactivex.functions.Consumer;
import io.reactivex.internal.fuseable.SimplePlainQueue;
import io.reactivex.internal.queue.MpscLinkedQueue;
import io.reactivex.plugins.RxJavaPlugins;

@SuppressWarnings("serial")
public final class SerializedConnectionListener extends AtomicInteger
        implements Consumer<Optional<Throwable>> {

    private final Consumer<? super Optional<Throwable>> c;
    private final SimplePlainQueue<Optional<Throwable>> queue = new MpscLinkedQueue<>();

    public SerializedConnectionListener(Consumer<? super Optional<Throwable>> c) {
        this.c = c;
    }

    @Override
    public void accept(Optional<Throwable> error) throws Exception {
        queue.offer(error);
        drain();
    }

    private void drain() {
        if (getAndIncrement() == 0) {
            int missed = 1;
            while (true) {
                Optional<Throwable> o;
                while ((o = queue.poll()) != null) {
                    try {
                        c.accept(o);
                    } catch (Exception e) {
                        RxJavaPlugins.onError(e);
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
