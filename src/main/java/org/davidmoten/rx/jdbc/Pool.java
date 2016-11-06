package org.davidmoten.rx.jdbc;

import java.util.concurrent.Callable;

import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.subjects.PublishSubject;

public class Pool<T> {

	private final Flowable<Member<T>> members;

	public Pool(Callable<T> factory, int maxSize) {
		PublishSubject<Member<T>> subject = PublishSubject.create();
		this.members = Flowable //
				.range(1, maxSize) //
				.map(n -> new Member<T>(factory.call(), subject)) //
				.mergeWith(subject.toFlowable(BackpressureStrategy.BUFFER)) //
				.share() //
				.filter(member -> member.checkout());
		//need at least one subscriber otherwise if subscribers got to zero then up again we will create another pool
		members.materialize().ignoreElements().subscribe();
	}

	public Flowable<Member<T>> members() {
		return members;
	}

}
