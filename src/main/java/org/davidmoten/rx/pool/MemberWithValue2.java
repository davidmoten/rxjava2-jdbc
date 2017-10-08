package org.davidmoten.rx.pool;

public interface MemberWithValue2<T> extends Member2<T>{
    /**
     * Should only be called if the Member has been checked out (managed by the
     * Pool instance).
     * 
     * @return the value of the pooled member
     */
    T value();
}
