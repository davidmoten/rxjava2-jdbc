package org.davidmoten.rx.pool;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import org.davidmoten.rx.internal.LifoQueue;
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
import io.reactivex.internal.util.EmptyComponent;
import io.reactivex.plugins.RxJavaPlugins;


final class MemberSingle<T> extends Single<Member<T>> implements Closeable {

    final Observers<T> observers;

    private static final Logger log = LoggerFactory.getLogger(MemberSingle.class);

    // sentinel object representing remove all observers that is added to
    // toBeRemoved queue
    private final MemberSingleObserver<T> removeAll;

    private final LifoQueue<DecoratingMember<T>> initializedAvailable;
    private final SimplePlainQueue<DecoratingMember<T>> notInitialized;
    private final SimplePlainQueue<DecoratingMember<T>> toBeReleased;
    private final SimplePlainQueue<DecoratingMember<T>> toBeChecked;
    private final SimplePlainQueue<MemberSingleObserver<T>> toBeAdded;
    private final SimplePlainQueue<MemberSingleObserver<T>> toBeRemoved;

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

    MemberSingle(NonBlockingPool<T> pool) {
        Preconditions.checkNotNull(pool);
        this.notInitialized = new MpscLinkedQueue<>();
        this.initializedAvailable = new LifoQueue<>();
        this.toBeReleased = new MpscLinkedQueue<>();
        this.toBeChecked = new MpscLinkedQueue<>();
        this.toBeAdded = new MpscLinkedQueue<>();
        this.toBeRemoved = new MpscLinkedQueue<>();
        this.members = createMembersArray(pool.maxSize, pool.checkinDecorator);
        for (DecoratingMember<T> m : members) {
            notInitialized.offer(m);
        }
        this.scheduler = pool.scheduler;
        this.createRetryIntervalMs = pool.createRetryIntervalMs;
        this.observers = new Observers<T>();
        this.pool = pool;
        this.removeAll = new MemberSingleObserver<T>(EmptyComponent.INSTANCE, this);
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
        log.debug("subscribeActual");
        // the action of checking out a member from the pool is implemented as a
        // subscription to the singleton MemberSingle
        MemberSingleObserver<T> o = new MemberSingleObserver<T>(observer, this);
        observer.onSubscribe(o);
        if (pool.isClosed()) {
            observer.onError(new PoolClosedException());
            return;
        }
        toBeAdded.offer(o);
        drain();
    }

    public void checkin(Member<T> member) {
        checkin(member, false);
    }

    public void checkin(Member<T> member, boolean decrementInitializeScheduled) {
        log.debug("checking in {}", member);
        DecoratingMember<T> d = (DecoratingMember<T>) member;
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
                // we add observers or schedule release of members even if no requests exist
                removeObservers();
                addObservers();

                scheduleReleasesNoDelay();
                scheduleChecksNoDelay();

                Observers<T> obs = observers;
                log.debug("requested={}", obs.requested);
                // max we can emit is the number of active (available) resources in pool
                long r = Math.min(obs.readyCount, obs.requested);
                long e = 0; // emitted
                while (e != r && obs.readyCount > 0) {
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
                            emit(obs, m);
                            log.debug("emitted");
                            e++;
                        }
                    }
                    // else otherwise leave off the initializedAvailable queue because it is being
                    // released or checked

                    removeObservers();
                    addObservers();
                    
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

    private void addObservers() {
        MemberSingleObserver<T> o;
        while ((o = toBeAdded.poll()) != null) {
            observers.add(o);
        }
    }

    private void removeObservers() {
        MemberSingleObserver<T> o;
        while ((o = toBeRemoved.poll()) != null) {
            if (o == removeAll) {
                observers.removeAll();
                return;
            } else {
                observers.remove(o);
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
        return shouldPerformHealthCheck(m, pool.idleTimeBeforeHealthCheckMs, now);
    }

    @VisibleForTesting
    static <T> boolean shouldPerformHealthCheck(DecoratingMember<T> m, long idleTimeBeforeHealthCheckMs, long now) {
        return idleTimeBeforeHealthCheckMs > 0 && now - m.lastCheckTime() >= idleTimeBeforeHealthCheckMs;
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

    private void emit(Observers<T> obs, DecoratingMember<T> m) {
        // note that tryEmit is protected by the drain method so will
        // not be run concurrently.
        // advance counter to the next and choose an Observer to emit to (round robin)

        // a precondition of this method is that obs.activeCount > 0 (enforced by drain
        // method)

        int index = obs.index;
        int nextIndex = (index + 1) % observers.ready.size();
        while (nextIndex != index && !observers.ready.get(nextIndex)) {
            nextIndex = (nextIndex + 1) % observers.ready.size();
        }
        observers.ready.set(nextIndex, Boolean.FALSE);
        observers.readyCount--;
        observers.requested--;
        MemberSingleObserver<T> oNext = obs.observers.get(nextIndex);
        // get a fresh worker each time so we jump threads to
        // break the stack-trace (a long-enough chain of
        // checkout-checkins could otherwise provoke stack
        // overflow)
        Worker worker = scheduler.createWorker();
        worker.schedule(new Emitter<T>(worker, oNext, m));
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

    private void removeAllObservers() {
        toBeRemoved.offer(removeAll);
        drain();
    }

    void remove(@NonNull MemberSingleObserver<T> inner) {
        toBeRemoved.offer(inner);
        drain();
    }

    private static final class Observers<T> {

        final List<MemberSingleObserver<T>> observers;

        // an observer is ready until it is emitted to
        final List<Boolean> ready;

        // the number of true values in the ready array
        // which is the number of observers that can be
        // emitted to
        int readyCount;

        // used as the starting point of the next check for an
        // observer to emit to (for a round-robin). Is 0 when no
        // observers
        int index;

        int requested;

        Observers() {
            observers = new ArrayList<>();
            ready = new ArrayList<>();
            readyCount = 0;
            index = 0;
            requested = 0;
        }

        void add(MemberSingleObserver<T> o) {
            observers.add(o);
            ready.add(Boolean.TRUE);
            readyCount++;
            requested++;
        }

        void remove(MemberSingleObserver<T> o) {
            int i = observers.indexOf(o);
            if (i == -1) {
                // not present
                return;
            }
            readyCount = ready.get(i) ? readyCount - 1 : readyCount;
            if (index >= i && index > 0) {
                index--;
            }
            observers.remove(i);
            ready.remove(i);
        }

        void removeAll() {
            observers.clear();
            ready.clear();
            readyCount = 0;
            index = 0;
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
            }
        }

        @Override
        public boolean isDisposed() {
            return get() == null;
        }
    }

}
