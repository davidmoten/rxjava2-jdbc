package org.davidmoten.rx.jdbc.pool;

import java.sql.Connection;

import org.davidmoten.rx.jdbc.ConnectionProvider;
import org.junit.Test;
import org.mockito.Mockito;

public class NonBlockingConnectionPoolTest {
    
    @Test(expected = IllegalArgumentException.class)
    public void testRejectSingletonConnectionProvider() {
        Connection con = Mockito.mock(Connection.class);
        NonBlockingConnectionPool.builder().connectionProvider(ConnectionProvider.from(con));
    }

}
