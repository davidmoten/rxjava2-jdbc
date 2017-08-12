package org.davidmoten.rx.pool;

import java.io.Closeable;
import java.io.IOException;
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

class MemberSingle<T> extends Single<Member2<T>> implements Subscription, Closeable {

	final AtomicReference<Observers<T>> observers;

	@SuppressWarnings({ "rawtypes", "unchecked" })
	static final Observers EMPTY = new Observers(new MemberSingleObserver[0], new boolean[0], 0, 0);

	private final SimplePlainQueue<Member2<T>> queue;
	private final AtomicInteger wip = new AtomicInteger();
	private final Member2<T>[] members;
	private final Scheduler scheduler;
	private final int maxSize;

	// mutable

	private volatile boolean cancelled;

	// number of members in the pool at the moment
	private int count;

	private final NonBlockingPool2<T> pool;

	@SuppressWarnings("unchecked")
	MemberSingle(NonBlockingPool2<T> pool) {
		this.queue = new MpscLinkedQueue<Member2<T>>();
		this.members = createMembersArray(pool);
		this.scheduler = pool.scheduler;
		this.maxSize = pool.maxSize;
		this.observers = new AtomicReference<>(EMPTY);
		this.count = 1;
		this.pool = pool;
		queue.offer(members[0]);
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
		System.out.println("checking in");
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

	@SuppressWarnings("resource")
	private void drain() {
		if (wip.getAndIncrement() == 0) {
			int missed = 1;
			while (true) {
				int c = 0;
				while (c < count) {
					if (cancelled) {
						queue.clear();
						return;
					}
					Observers<T> obs = observers.get();
					if (obs.activeCount == 0) {
						break;
					}
					final Member2<T> m = queue.poll();
					if (m == null) {
						if (count < maxSize) {
							// haven't used all the members of the pool yet
							queue.offer(members[count]);
							count++;
						} else {
							break;
						}
					} else {
						Member2<T> m2;
						if ((m2 = m.checkout()) != null) {
							emit(obs, m2);
						} else {
							// put back on the queue for consideration later
							queue.offer(m);
						}
					}
					c++;
				}
				missed = wip.addAndGet(-missed);
				if (missed == 0) {
					return;
				}
			}
		}
	}

	private void emit(Observers<T> obs, Member2<T> m) {
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
				active[index] = false;
				int nextIndex = (index + 1) % active.length;
				while (nextIndex != index && !active[nextIndex]) {
					nextIndex = (nextIndex + 1) % active.length;
				}
				if (observers.compareAndSet(x, new Observers<T>(x.observers, active, x.activeCount - 1, nextIndex))) {
					oNext = x.observers[nextIndex];
					break;
				}
			} else {
				break;
			}
		}
		Worker worker = scheduler.createWorker();
		worker.schedule(new Emitter<T>(worker, oNext, m));
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
			if (observers.compareAndSet(a, new Observers<T>(b, active, a.activeCount + 1, a.index))) {
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
				if (a.index > j) {
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
		private int activeCount;
		final int index;

		Observers(MemberSingleObserver<T>[] observers, boolean[] active, int activeCount, int index) {
			Preconditions.checkArgument(observers.length > 0 || index == 0, "index must be -1 for zero length array");
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
		private final Member2<T> m;

		Emitter(Worker worker, MemberSingleObserver<T> observer, Member2<T> m) {
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

	static final class MemberSingleObserver<T> extends AtomicReference<MemberSingle<T>> implements Disposable {
		private static final long serialVersionUID = -7650903191002190468L;

		final SingleObserver<? super Member2<T>> child;

		MemberSingleObserver(SingleObserver<? super Member2<T>> child, MemberSingle<T> parent) {
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
