package org.davidmoten.rx.jdbc;

import java.util.concurrent.Callable;

import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.subjects.PublishSubject;

public class Pool<T> {

    private final Callable<T> factory;
    private final Member<T>[] members;
    private final PublishSubject<Member<T>> subject = PublishSubject.create();

    @SuppressWarnings("unchecked")
    public Pool(Callable<T> factory, int maxSize) {
        this.factory = factory;
        this.members = (Member<T>[]) new Object[maxSize];
    }

    public Flowable<T> members() {
        return subject.toFlowable(BackpressureStrategy.BUFFER).share().filter(member -> member.checkout()).map(member -> member.value);
    }

    public boolean checkout() throws Exception {
        //TODO handle asynchronous calls
        for (int i = 0; i < members.length; i++) {
            if (members[i] == null) {
                members[i] = new Member<T>(factory.call());
                subject.onNext(members[i]);
                return true;
            } else if (members[i].checkout()) {
                subject.onNext(members[i]);
                return true;
            }
        }
        return false;
    }

    public void checkin(Member<T> member) {
        member.checkin();
    }

}
