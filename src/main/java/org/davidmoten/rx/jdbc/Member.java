package org.davidmoten.rx.jdbc;

import java.util.concurrent.atomic.AtomicBoolean;

final class Member<T> {

    final T value;
    private final AtomicBoolean inUse = new AtomicBoolean(true);

    public Member(T value) {
        this.value = value;
    }
    
    public boolean checkout() {
        return inUse.compareAndSet(false, true); 
    }
    
    public void checkin() {
        inUse.set(false);
    }

}