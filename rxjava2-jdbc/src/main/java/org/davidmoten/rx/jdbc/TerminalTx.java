package org.davidmoten.rx.jdbc;

public interface TerminalTx {

    boolean isComplete();

    boolean isError();

    Throwable throwable();
    
    TransactedSelectBuilder select(String sql);
    
    TransactedUpdateBuilder update(String sql);
    
}
