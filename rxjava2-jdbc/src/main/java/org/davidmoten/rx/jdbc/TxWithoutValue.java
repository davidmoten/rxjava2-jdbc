package org.davidmoten.rx.jdbc;

public interface TxWithoutValue {

    boolean isComplete();

    boolean isError();

    Throwable throwable();
    
    TransactedSelectBuilder select(String sql);
    
    TransactedUpdateBuilder update(String sql);
    
    TransactedCallableBuilder call(String sql);
    
}
