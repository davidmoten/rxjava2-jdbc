package org.davidmoten.rx.pool;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.davidmoten.guavamini.Preconditions;

import io.reactivex.Scheduler;
import io.reactivex.Scheduler.Worker;
import io.reactivex.Single;
import io.reactivex.SingleObserver;
import io.reactivex.annotations.NonNull;
import io.reactivex.disposables.CompositeDisposable;
import io.reactivex.disposables.Disposable;
import io.reactivex.internal.fuseable.SimplePlainQueue;
import io.reactivex.internal.queue.MpscLinkedQueue;
import io.reactivex.plugins.RxJavaPlugins;

class MemberSingle<T> extends Single<Member<T>> implements Subscription, Closeable, Runnable {

    final AtomicReference<Observers<T>> observers;

    private static final Logger log = LoggerFactory.getLogger(MemberSingle.class);

    @SuppressWarnings({ "rawtypes", "unchecked" })
    static final Observers EMPTY = new Observers(new MemberSingleObserver[0], new boolean[0], 0, 0);

    private final SimplePlainQueue<DecoratingMember<T>> initializedAvailable;
    private final SimplePlainQueue<DecoratingMember<T>> notInitialized;
    private final SimplePlainQueue<DecoratingMember<T>> toBeReleased;

    private final AtomicInteger wip = new AtomicInteger();
    private final DecoratingMember<T>[] members;
    private final Scheduler scheduler;
    private final long checkoutRetryIntervalMs;

    // mutable

    private volatile boolean cancelled;

    // synchronized by `wip`
    private final CompositeDisposable scheduled = new CompositeDisposable();

    final NonBlockingPool<T> pool;

    // represents the number of outstanding member requests.
    // the number is decremented when a new member value is
    // initialized (a scheduled action with a subsequent drain call)
    // or an existing value is available from the pool (queue) (and is then
    // emitted).
    private final AtomicLong requested = new AtomicLong();

    @SuppressWarnings("unchecked")
    MemberSingle(NonBlockingPool<T> pool) {
        Preconditions.checkNotNull(pool);
        this.initializedAvailable = new MpscLinkedQueue<DecoratingMember<T>>();
        this.notInitialized = new MpscLinkedQueue<DecoratingMember<T>>();
        this.toBeReleased = new MpscLinkedQueue<DecoratingMember<T>>();
        this.members = createMembersArray(pool.maxSize, pool.checkinDecorator);
        for (DecoratingMember<T> m : members) {
            notInitialized.offer(m);
        }
        this.scheduler = pool.scheduler;
        this.checkoutRetryIntervalMs = pool.checkoutRetryIntervalMs;
        this.observers = new AtomicReference<>(EMPTY);
        this.pool = pool;
    }

    private DecoratingMember<T>[] createMembersArray(int poolMaxSize,
            BiFunction<? super T, ? super Checkin, ? extends T> checkinDecorator) {
        @SuppressWarnings("unchecked")
        DecoratingMember<T>[] m = new DecoratingMember[poolMaxSize];
        for (int i = 0; i < m.length; i++) {
            m[i] = new DecoratingMember<T>(null, checkinDecorator, this);
        }
        return m;
    }

    @Override
    protected void subscribeActual(SingleObserver<? super Member<T>> observer) {
        // the action of checking out a member from the pool is implemented as a
        // subscription to the singleton MemberSingle
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
        requested.incrementAndGet();
        log.debug("subscribed");
        drain();
    }

    public void checkin(Member<T> member) {
        log.debug("checking in {}", member);
        ((DecoratingMember<T>) member).scheduleRelease();
        initializedAvailable.offer((DecoratingMember<T>) member);
        drain();
    }

    public void addToBeReleased(DecoratingMember<T> member) {
        toBeReleased.offer(member);
        drain();
    }

    @Override
    public void request(long n) {
        drain();
    }

    @Override
    public void cancel() {
        log.debug("cancel called");
        this.cancelled = true;
        disposeAll();
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
        log.debug("drain called");
        if (wip.getAndIncrement() == 0) {
            log.debug("drain loop starting");
            int missed = 1;
            while (true) {
                // schedule release of any member queued for releasing
                {
                    DecoratingMember<T> m;
                    while ((m = toBeReleased.poll()) != null) {
                        log.debug("scheduling release of {}", m);
                        m.markAsReleasing();
                        scheduled.add(scheduler.scheduleDirect(new Releaser<T>(m)));
                    }
                }
                long r = requested.get();
                log.debug("requested={}", r);
                long e = 0;
                while (e != r) {
                    if (cancelled) {
                        disposeAll();
                        return;
                    }
                    Observers<T> obs = observers.get();
                    // the check below required so a tryEmit that returns false doesn't bring about
                    // a spin on this loop
                    if (obs.activeCount == 0) {
                        break;
                    }
                    // check for an already initialized available member
                    final DecoratingMember<T> m = initializedAvailable.poll();
                    log.debug("poll of available members returns " + m);
                    if (m == null) {
                        // no members available, check for a released member (that needs to be
                        // reinitialized before use)
                        final DecoratingMember<T> m2 = notInitialized.poll();
                        if (m2 == null) {
                            break;
                        } else {
                            // incrementing e here will result in requested being decremented
                            // (outside of this loop). After scheduled creation has occurred
                            // requested will be incremented and drain called.
                            e++;
                            log.debug("scheduling member creation");
                            scheduled.add(scheduleCreateValue(m2));
                        }
                    } else if (!m.isReleasing()) {
                        // this should not block because it just schedules emissions to observers
                        log.debug("trying to emit member");
                        if (tryEmit(obs, m)) {
                            e++;
                        }
                    }
                }
                if (e != 0L && r != Long.MAX_VALUE) {
                    requested.addAndGet(-e);
                }
                missed = wip.addAndGet(-missed);
                if (missed == 0) {
                    return;
                }
            }
        }
    }

    private void disposeAll() {
        initializedAvailable.clear();
        toBeReleased.clear();
        notInitialized.clear();
        disposeValues();
        removeAllObservers();
    }

    static final class Releaser<T> implements Runnable {

        private DecoratingMember<T> m;

        Releaser(DecoratingMember<T> m) {
            this.m = m;
        }

        @Override
        public void run() {
            try {
                m.release();
            } catch (Throwable t) {
                RxJavaPlugins.onError(t);
            }
        }
    }

    private Disposable scheduleCreateValue(DecoratingMember<T> m) {
        return scheduler.scheduleDirect(() -> {
            if (!cancelled) {
                try {
                    log.debug("creating value");
                    // this action might block so is scheduled
                    T value = pool.factory.call();
                    m.setValueAndClearReleasingFlag(value);
                    requested.incrementAndGet();
                    checkin(m);
                } catch (Throwable t) {
                    RxJavaPlugins.onError(t);
                    // check cancelled again because factory.call() is user specified and could have
                    // taken a significant time to complete
                    if (!cancelled) {
                        // schedule a retry
                        scheduled.add(scheduler.scheduleDirect(this, checkoutRetryIntervalMs,
                                TimeUnit.MILLISECONDS));
                    }
                }
            }
        });
    }

    private boolean tryEmit(Observers<T> obs, DecoratingMember<T> m) {
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
                // checkin because no active observers
                m.checkin();
                return false;
            }
        }
        Worker worker = scheduler.createWorker();
        worker.schedule(new Emitter<T>(worker, oNext, m));
        return true;
    }

    @Override
    public void close() {
        cancel();
    }

    private void disposeValues() {
        scheduled.dispose();
        for (DecoratingMember<T> member : members) {
            member.disposeValue();
        }
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
    private void removeAllObservers() {
        while (true) {
            Observers<T> a = observers.get();
            if (observers.compareAndSet(a, EMPTY)) {
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
            } catch (Throwable e) {
                RxJavaPlugins.onError(e);
            } finally {
                observer.dispose();
            }
        }
    }

    public void release(DecoratingMember<T> m) {
        log.debug("adding released member to notInitialized queue {}", m);
        notInitialized.offer(m);
        drain();
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
