package org.davidmoten.rx.pool;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.davidmoten.rx.jdbc.pool.PoolClosedException;
import org.reactivestreams.Subscription;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Scheduler;
import io.reactivex.Scheduler.Worker;
import io.reactivex.Single;
import io.reactivex.SingleObserver;
import io.reactivex.annotations.NonNull;
import io.reactivex.disposables.Disposable;
import io.reactivex.internal.fuseable.SimplePlainQueue;
import io.reactivex.internal.queue.MpscLinkedQueue;
import io.reactivex.plugins.RxJavaPlugins;

class MemberSingle<T> extends Single<Member<T>> implements Subscription, Closeable, Runnable {

    final AtomicReference<Observers<T>> observers;

    @SuppressWarnings({ "rawtypes", "unchecked" })
    static final Observers EMPTY = new Observers(new MemberSingleObserver[0], new boolean[0], 0, 0);

    private final SimplePlainQueue<Member<T>> queue;
    private final AtomicInteger wip = new AtomicInteger();
    private final Member<T>[] members;
    private final Scheduler scheduler;
    private final int maxSize;

    // mutable

    private volatile boolean cancelled;

    // number of members in the pool at the moment
    // synchronized by `wip`
    private int count;

    // synchronized by `wip`
    private Disposable scheduledDrain;

    private final NonBlockingPool<T> pool;

    private final long checkoutRetryIntervalMs;

    @SuppressWarnings("unchecked")
    MemberSingle(NonBlockingPool<T> pool) {
        this.queue = new MpscLinkedQueue<Member<T>>();
        this.members = createMembersArray(pool);
        this.scheduler = pool.scheduler;
        this.maxSize = pool.maxSize;
        this.observers = new AtomicReference<>(EMPTY);
        this.count = 1;
        this.pool = pool;
        this.checkoutRetryIntervalMs = pool.checkoutRetryIntervalMs;
        queue.offer(members[0]);
    }

    private static <T> Member<T>[] createMembersArray(NonBlockingPool<T> pool) {
        @SuppressWarnings("unchecked")
        Member<T>[] m = new Member[pool.maxSize];
        for (int i = 0; i < m.length; i++) {
            m[i] = pool.memberFactory.create(pool);
        }
        return m;
    }

    public void checkin(Member<T> member) {
        queue.offer(member);
        drain();
    }

    @Override
    public void request(long n) {
        drain();
    }

    @Override
    public void cancel() {
        this.cancelled = true;
    }

    @Override
    public void run() {
        try {
            drain();
        } catch (Throwable t) {
            RxJavaPlugins.onError(t);
        }
    }

    private void drain() {
        if (wip.getAndIncrement() == 0) {
            int missed = 1;
            while (true) {
                Member<T> couldNotCheckout = null;
                if (scheduledDrain != null) {
                    // if a drain has been scheduled then cancel it because we are draining now
                    scheduledDrain.dispose();
                    scheduledDrain = null;
                }
                while (true) {
                    if (cancelled) {
                        queue.clear();
                        return;
                    }
                    Observers<T> obs = observers.get();
                    if (obs.activeCount == 0) {
                        break;
                    }
                    final Member<T> m = queue.poll();
                    if (m == null) {
                        if (count < maxSize) {
                            // haven't used all the members of the pool yet
                            queue.offer(members[count]);
                            count++;
                        } else {
                            break;
                        }
                    } else {
                        Member<T> m2;
                        if ((m2 = m.checkout()) != null) {
                            emit(obs, m2);
                        } else if (m.isShutdown()) {
                            // don't return m to the queue and continue
                        } else {
                            // put back on the queue for consideration later
                            // if not shutdown
                            queue.offer(m);
                            if (couldNotCheckout == null) {
                                couldNotCheckout = m;
                            } else if (couldNotCheckout == m) {
                                // this m has failed to checkout before in this loop
                                // which could happen if database is not available for instance
                                // so let's try again after an interval
                                scheduledDrain = scheduler.scheduleDirect(this,
                                        checkoutRetryIntervalMs, TimeUnit.MILLISECONDS);
                                break;
                            }
                        }
                    }
                }
                missed = wip.addAndGet(-missed);
                if (missed == 0) {
                    return;
                }
            }
        }
    }

    private void emit(Observers<T> obs, Member<T> m) {
        // get a fresh worker each time so we jump threads to
        // break the stack-trace (a long-enough chain of
        // checkout-checkins could otherwise provoke stack
        // overflow)

        // advance counter so the next and choose an Observer to emit to (round robin)

        int index = obs.index;
        MemberSingleObserver<T> o = obs.observers[index];
        MemberSingleObserver<T> oNext = o;
        // atomically bump up the index (if that entry has not been deleted in
        // the meantime by disposal)
        while (true) {
            Observers<T> x = observers.get();
            if (x.index == index && x.observers[index] == o) {
                boolean[] active = new boolean[x.active.length];
                System.arraycopy(x.active, 0, active, 0, active.length);
                int nextIndex = (index + 1) % active.length;
                while (nextIndex != index && !active[nextIndex]) {
                    nextIndex = (nextIndex + 1) % active.length;
                }
                active[nextIndex] = false;
                if (observers.compareAndSet(x,
                        new Observers<T>(x.observers, active, x.activeCount - 1, nextIndex))) {
                    oNext = x.observers[nextIndex];
                    break;
                }
            } else {
                m.checkin();
                return;
            }
        }
        Worker worker = scheduler.createWorker();
        worker.schedule(new Emitter<T>(worker, oNext, m));
        return;
    }

    @Override
    public void close() throws IOException {
        for (Member<T> member : members) {
            try {
                member.close();
            } catch (Exception e) {
                // TODO accumulate and throw?
                e.printStackTrace();
            }
        }
    }

    @Override
    protected void subscribeActual(SingleObserver<? super Member<T>> observer) {
        MemberSingleObserver<T> md = new MemberSingleObserver<T>(observer, this);
        observer.onSubscribe(md);
        if (pool.isClosed()) {
            observer.onError(new PoolClosedException());
            return;
        }
        add(md);
        if (md.isDisposed()) {
            remove(md);
        }
        drain();
    }

    void add(@NonNull MemberSingleObserver<T> inner) {
        while (true) {
            Observers<T> a = observers.get();
            int n = a.observers.length;
            @SuppressWarnings("unchecked")
            MemberSingleObserver<T>[] b = new MemberSingleObserver[n + 1];
            System.arraycopy(a.observers, 0, b, 0, n);
            b[n] = inner;
            boolean[] active = new boolean[n + 1];
            System.arraycopy(a.active, 0, active, 0, n);
            active[n] = true;
            if (observers.compareAndSet(a,
                    new Observers<T>(b, active, a.activeCount + 1, a.index))) {
                return;
            }
        }
    }

    @SuppressWarnings("unchecked")
    void remove(@NonNull MemberSingleObserver<T> inner) {
        while (true) {
            Observers<T> a = observers.get();
            int n = a.observers.length;
            if (n == 0) {
                return;
            }

            int j = -1;

            for (int i = 0; i < n; i++) {
                if (a.observers[i] == inner) {
                    j = i;
                    break;
                }
            }

            if (j < 0) {
                return;
            }
            Observers<T> next;
            if (n == 1) {
                next = EMPTY;
            } else {
                MemberSingleObserver<T>[] b = new MemberSingleObserver[n - 1];
                System.arraycopy(a.observers, 0, b, 0, j);
                System.arraycopy(a.observers, j + 1, b, j, n - j - 1);
                boolean[] active = new boolean[n - 1];
                System.arraycopy(a.active, 0, active, 0, j);
                System.arraycopy(a.active, j + 1, active, j, n - j - 1);
                int nextActiveCount = a.active[j] ? a.activeCount - 1 : a.activeCount;
                if (a.index >= j && a.index > 0) {
                    next = new Observers<T>(b, active, nextActiveCount, a.index - 1);
                } else {
                    next = new Observers<T>(b, active, nextActiveCount, a.index);
                }
            }
            if (observers.compareAndSet(a, next)) {
                return;
            }
        }
    }

    private static final class Observers<T> {
        final MemberSingleObserver<T>[] observers;
        // an observer is active until it is emitted to
        final boolean[] active;
        final int activeCount;
        final int index;

        Observers(MemberSingleObserver<T>[] observers, boolean[] active, int activeCount,
                int index) {
            Preconditions.checkArgument(observers.length > 0 || index == 0,
                    "index must be -1 for zero length array");
            Preconditions.checkArgument(observers.length == active.length);
            this.observers = observers;
            this.index = index;
            this.active = active;
            this.activeCount = activeCount;
        }
    }

    private static final class Emitter<T> implements Runnable {

        private final Worker worker;
        private final MemberSingleObserver<T> observer;
        private final Member<T> m;

        Emitter(Worker worker, MemberSingleObserver<T> observer, Member<T> m) {
            this.worker = worker;
            this.observer = observer;
            this.m = m;
        }

        @Override
        public void run() {
            worker.dispose();
            try {
                observer.child.onSuccess(m);
                observer.dispose();
            } catch (Throwable e) {
                RxJavaPlugins.onError(e);
            }
        }
    }

    static final class MemberSingleObserver<T> extends AtomicReference<MemberSingle<T>>
            implements Disposable {
        private static final long serialVersionUID = -7650903191002190468L;

        final SingleObserver<? super Member<T>> child;

        MemberSingleObserver(SingleObserver<? super Member<T>> child, MemberSingle<T> parent) {
            this.child = child;
            lazySet(parent);
        }

        @Override
        public void dispose() {
            MemberSingle<T> parent = getAndSet(null);
            if (parent != null) {
                parent.remove(this);
            }
        }

        @Override
        public boolean isDisposed() {
            return get() == null;
        }
    }

}
