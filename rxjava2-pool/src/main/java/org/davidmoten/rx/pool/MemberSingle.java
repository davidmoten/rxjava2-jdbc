package org.davidmoten.rx.pool;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.davidmoten.guavamini.Preconditions;
import com.github.davidmoten.guavamini.annotations.VisibleForTesting;

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

final class MemberSingle<T> extends Single<Member<T>> implements Closeable {

    final AtomicReference<Observers<T>> observers;

    private static final Logger log = LoggerFactory.getLogger(MemberSingle.class);

    @SuppressWarnings({ "rawtypes", "unchecked" })
    static final Observers EMPTY = new Observers(new MemberSingleObserver[0], new boolean[0], 0, 0, 0);

    private final SimplePlainQueue<DecoratingMember<T>> initializedAvailable;
    private final SimplePlainQueue<DecoratingMember<T>> notInitialized;
    private final SimplePlainQueue<DecoratingMember<T>> toBeReleased;
    private final SimplePlainQueue<DecoratingMember<T>> toBeChecked;

    private final AtomicInteger wip = new AtomicInteger();
    private final DecoratingMember<T>[] members;
    private final Scheduler scheduler;
    private final long createRetryIntervalMs;

    // synchronized by `wip`
    private final CompositeDisposable scheduled = new CompositeDisposable();

    final NonBlockingPool<T> pool;

    private final AtomicLong initializeScheduled = new AtomicLong();

    // mutable
    private volatile boolean cancelled;

    @SuppressWarnings("unchecked")
    MemberSingle(NonBlockingPool<T> pool) {
        Preconditions.checkNotNull(pool);
        this.initializedAvailable = new MpscLinkedQueue<DecoratingMember<T>>();
        this.notInitialized = new MpscLinkedQueue<DecoratingMember<T>>();
        this.toBeReleased = new MpscLinkedQueue<DecoratingMember<T>>();
        this.toBeChecked = new MpscLinkedQueue<DecoratingMember<T>>();
        this.members = createMembersArray(pool.maxSize, pool.checkinDecorator);
        for (DecoratingMember<T> m : members) {
            notInitialized.offer(m);
        }
        this.scheduler = pool.scheduler;
        this.createRetryIntervalMs = pool.createRetryIntervalMs;
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
        MemberSingleObserver<T> m = new MemberSingleObserver<T>(observer, this);
        observer.onSubscribe(m);
        if (pool.isClosed()) {
            observer.onError(new PoolClosedException());
            return;
        }
        add(m);
        if (m.isDisposed()) {
            remove(m);
        } else {
            // atomically change requested
            while (true) {
                Observers<T> a = observers.get();
                if (observers.compareAndSet(a, a.withRequested(a.requested + 1))) {
                    break;
                }
            }
        }
        log.debug("subscribed");
        drain();
    }

    public void checkin(Member<T> member) {
        checkin(member, false);
    }

    public void checkin(Member<T> member, boolean decrementInitializeScheduled) {
        log.debug("checking in {}", member);
        DecoratingMember<T> d = ((DecoratingMember<T>) member);
        d.scheduleRelease();
        d.markAsChecked();
        initializedAvailable.offer((DecoratingMember<T>) member);
        if (decrementInitializeScheduled) {
            initializeScheduled.decrementAndGet();
        }
        drain();
    }

    public void addToBeReleased(DecoratingMember<T> member) {
        toBeReleased.offer(member);
        drain();
    }

    public void cancel() {
        log.debug("cancel called");
        this.cancelled = true;
        disposeAll();
    }

    private void drain() {
        log.debug("drain called");
        if (wip.getAndIncrement() == 0) {
            log.debug("drain loop starting");
            int missed = 1;
            while (true) {
                // we schedule release of members even if no requests exist
                scheduleReleasesNoDelay();
                scheduleChecksNoDelay();

                Observers<T> obs = observers.get();
                log.debug("requested={}", obs.requested);
                // max we can emit is the number of active (available) resources in pool
                long r = Math.min(obs.activeCount, obs.requested);
                long e = 0; // emitted
                // record number of attempted emits in case all observers have been cancelled
                // while are in the loop and we want to break
                long attempts = 0;
                while (e != r && attempts != obs.activeCount) {
                    if (cancelled) {
                        disposeAll();
                        return;
                    }
                    // check for an already initialized available member
                    final DecoratingMember<T> m = initializedAvailable.poll();
                    log.debug("poll of available members returns {}", m);
                    if (m == null) {
                        // no members available, check for a released member (that needs to be
                        // reinitialized before use)
                        final DecoratingMember<T> m2 = notInitialized.poll();
                        if (m2 == null) {
                            break;
                        } else {
                            // only schedule member initialization if there is enough demand,
                            boolean used = trySchedulingInitializationNoDelay(r, e, m2);
                            if (!used) {
                                break;
                            }
                        }
                    } else if (!m.isReleasing() && !m.isChecking()) {
                        log.debug("trying to emit member");
                        if (shouldPerformHealthCheck(m)) {
                            log.debug("queueing member for health check {}", m);
                            toBeChecked.offer(m);
                        } else {
                            log.debug("no health check required for {}", m);
                            // this should not block because it just schedules emissions to observers
                            if (tryEmit(obs, m)) {
                                e++;
                            } else {
                                log.debug("no active observers");
                            }
                            attempts++;
                        }
                    }
                    // schedule release immediately of any member
                    // queued for releasing
                    scheduleReleasesNoDelay();
                    // schedule check of any member queued for checking
                    scheduleChecksNoDelay();
                }
                missed = wip.addAndGet(-missed);
                if (missed == 0) {
                    return;
                }
            }
        }
    }

    private boolean trySchedulingInitializationNoDelay(long r, long e, final DecoratingMember<T> m) {
        // check initializeScheduled using a CAS loop
        while (true) {
            long cs = initializeScheduled.get();
            if (e + cs < r) {
                if (initializeScheduled.compareAndSet(cs, cs + 1)) {
                    log.debug("scheduling member creation");
                    scheduled.add(scheduler.scheduleDirect(new Initializer(m)));
                    return true;
                }
            } else {
                log.debug("insufficient demand to initialize {}", m);
                // don't need to initialize more so put back on queue and exit the loop
                notInitialized.offer(m);
                return false;
            }
        }
    }

    private boolean shouldPerformHealthCheck(final DecoratingMember<T> m) {
        long now = scheduler.now(TimeUnit.MILLISECONDS);
        log.debug("schedule.now={}, lastCheck={}", now, m.lastCheckTime());
        return pool.idleTimeBeforeHealthCheckMs > 0 && now - m.lastCheckTime() >= pool.idleTimeBeforeHealthCheckMs;
    }

    private void scheduleChecksNoDelay() {
        DecoratingMember<T> m;
        while ((m = toBeChecked.poll()) != null) {
            if (!m.isReleasing()) {
                log.debug("scheduling check of {}", m);
                // we mark as checking so that we can ignore it if already in the
                // initializedAvailable queue after concurrent checkin
                m.markAsChecking();
                scheduled.add(scheduler.scheduleDirect(new Checker(m)));
            }
        }
    }

    private void scheduleReleasesNoDelay() {
        DecoratingMember<T> m;
        while ((m = toBeReleased.poll()) != null) {
            log.debug("scheduling release of {}", m);
            // we mark as releasing so that we can ignore it if already in the
            // initializedAvailable queue after concurrent checkin
            m.markAsReleasing();
            scheduled.add(scheduler.scheduleDirect(new Releaser(m)));
        }
    }

    private boolean tryEmit(Observers<T> obs, DecoratingMember<T> m) {
        // note that tryEmit is protected by the drain method so will 
        // not be run concurrently. We do have to be careful with 
        // concurrent disposal of observers though.
        
        
        // advance counter to the next and choose an Observer to emit to (round robin)

        int index = obs.index;
        // a precondition of this method is that obs.activeCount > 0 (enforced by drain method)
        MemberSingleObserver<T> o = obs.observers[index];
        MemberSingleObserver<T> oNext = o;
        
        // atomically bump up the index to select the next Observer by round-robin
        // (if that entry has not been deleted in the meantime by disposal). Need 
        // to be careful too that ALL observers have not been deleted via a race 
        // with disposal.
        while (true) {
            Observers<T> x = observers.get();
            if (x.index == index && x.activeCount > 0 && x.observers[index] == o) {
                boolean[] active = new boolean[x.active.length];
                System.arraycopy(x.active, 0, active, 0, active.length);
                int nextIndex = (index + 1) % active.length;
                while (nextIndex != index && !active[nextIndex]) {
                    nextIndex = (nextIndex + 1) % active.length;
                }
                active[nextIndex] = false;
                if (observers.compareAndSet(x,
                        new Observers<T>(x.observers, active, x.activeCount - 1, nextIndex, x.requested - 1))) {
                    oNext = x.observers[nextIndex];
                    break;
                }
            } else {
                // checkin because no active observers
                m.checkin();
                return false;
            }
        }
        // get a fresh worker each time so we jump threads to
        // break the stack-trace (a long-enough chain of
        // checkout-checkins could otherwise provoke stack
        // overflow)
        Worker worker = scheduler.createWorker();
        worker.schedule(new Emitter<T>(worker, oNext, m));
        return true;
    }

    @VisibleForTesting
    final class Initializer implements Runnable {

        private final DecoratingMember<T> m;

        Initializer(DecoratingMember<T> m) {
            this.m = m;
        }

        @Override
        public void run() {
            if (!cancelled) {
                try {
                    log.debug("creating value");
                    // this action might block so is scheduled
                    T value = pool.factory.call();
                    m.setValueAndClearReleasingFlag(value);
                    checkin(m, true);
                } catch (Throwable t) {
                    RxJavaPlugins.onError(t);
                    // check cancelled again because factory.call() is user specified and could have
                    // taken a significant time to complete
                    if (!cancelled) {
                        // schedule a retry
                        scheduled.add(scheduler.scheduleDirect(this, createRetryIntervalMs, TimeUnit.MILLISECONDS));
                    }
                }
            }
        }
    }

    final class Releaser implements Runnable {

        private DecoratingMember<T> m;

        Releaser(DecoratingMember<T> m) {
            this.m = m;
        }

        @Override
        public void run() {
            try {
                m.disposeValue();
                release(m);
            } catch (Throwable t) {
                RxJavaPlugins.onError(t);
            }
        }
    }

    final class Checker implements Runnable {

        private final DecoratingMember<T> m;

        Checker(DecoratingMember<T> m) {
            this.m = m;
        }

        @Override
        public void run() {
            try {
                log.debug("performing health check on {}", m);
                if (!pool.healthCheck.test(m.value())) {
                    log.debug("failed health check");
                    m.disposeValue();
                    log.debug("scheduling recreation of member {}", m);
                    scheduled.add(scheduler.scheduleDirect(() -> {
                        log.debug("recreating member after failed health check {}", m);
                        notInitialized.offer(m);
                        drain();
                    }, pool.createRetryIntervalMs, TimeUnit.MILLISECONDS));
                } else {
                    m.markAsChecked();
                    initializedAvailable.offer(m);
                    drain();
                }
            } catch (Throwable t) {
                RxJavaPlugins.onError(t);
            }
        }
    }

    @Override
    public void close() {
        cancel();
    }

    private void disposeAll() {
        initializedAvailable.clear();
        toBeReleased.clear();
        notInitialized.clear();
        disposeValues();
        removeAllObservers();
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
            if (observers.compareAndSet(a, new Observers<T>(b, active, a.activeCount + 1, a.index, a.requested))) {
                return;
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void removeAllObservers() {
        while (true) {
            Observers<T> a = observers.get();
            if (observers.compareAndSet(a, EMPTY.withRequested(a.requested))) {
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
                next = EMPTY.withRequested(a.requested);
            } else {
                MemberSingleObserver<T>[] b = new MemberSingleObserver[n - 1];
                System.arraycopy(a.observers, 0, b, 0, j);
                System.arraycopy(a.observers, j + 1, b, j, n - j - 1);
                boolean[] active = new boolean[n - 1];
                System.arraycopy(a.active, 0, active, 0, j);
                System.arraycopy(a.active, j + 1, active, j, n - j - 1);
                int nextActiveCount = a.active[j] ? a.activeCount - 1 : a.activeCount;
                if (a.index >= j && a.index > 0) {
                    next = new Observers<T>(b, active, nextActiveCount, a.index - 1, a.requested);
                } else {
                    next = new Observers<T>(b, active, nextActiveCount, a.index, a.requested);
                }
            }
            if (observers.compareAndSet(a, next)) {
                break;
            }
        }
    }

    private static final class Observers<T> {
        
        final MemberSingleObserver<T>[] observers;
        
        // an observer is active until it is emitted to
        final boolean[] active;
        
        // the number of true values in the active array
        final int activeCount;
        
        final int index;
        final int requested;

        Observers(MemberSingleObserver<T>[] observers, boolean[] active, int activeCount, int index, int requested) {
            Preconditions.checkArgument(observers.length > 0 || index == 0, "index must be 0 for zero length array");
            Preconditions.checkArgument(observers.length == active.length);
            this.observers = observers;
            this.index = index;
            this.active = active;
            this.activeCount = activeCount;
            this.requested = requested;
        }

        Observers<T> withRequested(int r) {
            return new Observers<T>(observers, active, activeCount, index, r);
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

    static final class MemberSingleObserver<T> extends AtomicReference<MemberSingle<T>> implements Disposable {

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
                parent.drain();
            }
        }

        @Override
        public boolean isDisposed() {
            return get() == null;
        }
    }

}
