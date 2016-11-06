package org.davidmoten.rx.jdbc;

import java.util.concurrent.atomic.AtomicBoolean;

import io.reactivex.subjects.PublishSubject;

public final class Member<T> {

	private final T value;
	private final AtomicBoolean inUse = new AtomicBoolean(false);
	private final PublishSubject<Member<T>> subject;

	public Member(T value, PublishSubject<Member<T>> subject) {
		this.value = value;
		this.subject = subject;
	}

	public boolean checkout() {
		return inUse.compareAndSet(false, true);
	}

	public void checkin() {
        inUse.set(false);
        subject.onNext(this);
    }
	
	public T value(){
		return value;
	}

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Member [value=");
        builder.append(value);
        builder.append(", inUse=");
        builder.append(inUse);
        builder.append("]");
        return builder.toString();
    }

}