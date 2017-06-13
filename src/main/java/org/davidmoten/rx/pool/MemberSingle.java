package org.davidmoten.rx.pool;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivex.Scheduler;
import io.reactivex.Scheduler.Worker;
import io.reactivex.Single;
import io.reactivex.SingleObserver;
import io.reactivex.annotations.NonNull;
import io.reactivex.disposables.Disposable;
import io.reactivex.internal.fuseable.SimplePlainQueue;
import io.reactivex.internal.queue.MpscLinkedQueue;

class MemberSingle<T> extends Single<Member2<T>> implements Subscription, Closeable {

    final AtomicReference<SingleDisposable<T>[]> observers;

    @SuppressWarnings("rawtypes")
    static final SingleDisposable[] EMPTY = new SingleDisposable[0];

    private final SimplePlainQueue<Member2<T>> queue;
    private final AtomicInteger wip = new AtomicInteger();
    private final Member2<T>[] members;
    private final Scheduler scheduler;
    private final int maxSize;

    // mutable

    private volatile boolean cancelled;

    // number of members in the pool at the moment
    private int count;

    // index of the current observer
    private int index;

    @SuppressWarnings("unchecked")
    MemberSingle(NonBlockingPool2<T> pool) {
        this.queue = new MpscLinkedQueue<Member2<T>>();
        this.members = createMembersArray(pool);
        this.count = 0;
        this.scheduler = pool.scheduler;
        this.maxSize = pool.maxSize;
        this.observers = new AtomicReference<SingleDisposable<T>[]>(EMPTY);
    }

    private static <T> Member2<T>[] createMembersArray(NonBlockingPool2<T> pool) {
        @SuppressWarnings("unchecked")
        Member2<T>[] m = new Member2[pool.maxSize];
        for (int i = 0; i < m.length; i++) {
            m[i] = pool.memberFactory.create(pool);
        }
        return m;
    }

    public void checkin(Member2<T> member) {
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

    private void drain() {
        if (wip.getAndIncrement() == 0) {
            int missed = 0;
            while (true) {
                while (true) {
                    if (cancelled) {
                        queue.clear();
                        return;
                    }
                    SingleDisposable<T>[] obs = observers.get();
                    if (obs.length == 0) {
                        break;
                    }
                    Member2<T> m = queue.poll();
                    if (m == null) {
                        if (count < maxSize) {
                            // haven't used all the members of the pool yet
                            emit(obs, members[count]);
                            count++;
                        } else {
                            // nothing to emit and not done
                            break;
                        }
                    } else {
                        emit(obs, m);
                    }
                }
                missed = wip.addAndGet(-missed);
                if (missed == 0) {
                    return;
                }
            }
        }
    }

    private void emit(SingleDisposable<T>[] obs, Member2<T> m) {
        // get a fresh worker each time so we jump threads to
        // break the stack-trace (a long-enough chain of
        // checkout-checkins could otherwise provoke stack
        // overflow)

        // TODO choose an observer to emit to and advance counter so the next
        // subscriber in the list receives the next emission (round robin)

        // obs.length > 0
        index = index % obs.length;
        SingleDisposable<T> o = obs[index];
        index++;
        Worker worker = scheduler.createWorker();
        worker.schedule(new Emitter<T>(worker, o.actual, m));
    }

    @Override
    public void close() throws IOException {
        for (Member2<T> member : members) {
            try {
                member.close();
            } catch (Exception e) {
                // TODO accumulate and throw?
                e.printStackTrace();
            }
        }
    }

    @Override
    protected void subscribeActual(SingleObserver<? super Member2<T>> observer) {
        SingleDisposable<T> md = new SingleDisposable<T>(observer, this);
        observer.onSubscribe(md);
        add(md);
        if (md.isDisposed()) {
            remove(md);
        }
        drain();
    }

    void add(@NonNull SingleDisposable<T> inner) {
        for (;;) {
            SingleDisposable<T>[] a = observers.get();
            int n = a.length;
            @SuppressWarnings("unchecked")
            SingleDisposable<T>[] b = new SingleDisposable[n + 1];
            System.arraycopy(a, 0, b, 0, n);
            b[n] = inner;
            if (observers.compareAndSet(a, b)) {
                return;
            }
        }
    }

    @SuppressWarnings("unchecked")
    void remove(@NonNull SingleDisposable<T> inner) {
        for (;;) {
            SingleDisposable<T>[] a = observers.get();
            int n = a.length;
            if (n == 0) {
                return;
            }

            int j = -1;

            for (int i = 0; i < n; i++) {
                if (a[i] == inner) {
                    j = i;
                    break;
                }
            }

            if (j < 0) {
                return;
            }
            SingleDisposable<T>[] b;
            if (n == 1) {
                b = EMPTY;
            } else {
                b = new SingleDisposable[n - 1];
                System.arraycopy(a, 0, b, 0, j);
                System.arraycopy(a, j + 1, b, j, n - j - 1);
            }

            if (observers.compareAndSet(a, b)) {
                return;
            }
        }
    }

    private static final class Emitter<T> implements Runnable {

        private final Worker worker;
        private final SingleObserver<? super Member2<T>> child;
        private final Member2<T> m;

        Emitter(Worker worker, SingleObserver<? super Member2<T>> child, Member2<T> m) {
            this.worker = worker;
            this.child = child;
            this.m = m;
        }

        @Override
        public void run() {
            child.onSuccess(m);
            worker.dispose();
        }
    }

    static final class SingleDisposable<T> extends AtomicReference<MemberSingle<T>> implements Disposable {
        private static final long serialVersionUID = -7650903191002190468L;

        final SingleObserver<? super Member2<T>> actual;

        SingleDisposable(SingleObserver<? super Member2<T>> child, MemberSingle<T> parent) {
            this.actual = child;
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
