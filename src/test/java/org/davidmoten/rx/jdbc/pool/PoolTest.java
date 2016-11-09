package org.davidmoten.rx.jdbc.pool;

import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.davidmoten.rx.jdbc.Database;
import org.davidmoten.rx.pool.MemberFactory;
import org.davidmoten.rx.pool.NonBlockingMember;
import org.davidmoten.rx.pool.NonBlockingPool;
import org.davidmoten.rx.pool.Pool;
import org.junit.Test;

import io.reactivex.schedulers.Schedulers;
import io.reactivex.subscribers.TestSubscriber;

public class PoolTest {

	@Test
	public void test() throws InterruptedException {
		AtomicInteger count = new AtomicInteger();
		MemberFactory<Integer, NonBlockingPool<Integer>> memberFactory = pool -> new NonBlockingMember<Integer>(pool,
				null);
		Pool<Integer> pool = NonBlockingPool.factory(() -> count.incrementAndGet()).healthy(n -> true) //
				.disposer(n -> {
				}) //
				.maxSize(3)//
				.retryDelayMs(1000)//
				.memberFactory(memberFactory)//
				.scheduler(Schedulers.computation())//
				.build();
		pool.members() //
				.doOnNext(m -> m.checkin()) //
				.doOnNext(System.out::println) //
		        .test().awaitTerminalEvent();
	}

	@Test
	public void testConnectionPoolRecylesFirst() {
		Database db = DatabaseCreator.create(2);
		TestSubscriber<Connection> ts = db.connections() //
				.doOnNext(System.out::println) //
				.doOnNext(c -> {
					//release connection for reuse straight away
					c.close();
				}) //
				.test(4); //
		ts.assertValueCount(4) //
				.assertNotTerminated();
		List<Object> list = ts.getEvents().get(0);
		//all 4 connections released were the same
		assertTrue(list.get(0)==list.get(1));
		assertTrue(list.get(0)==list.get(2));
		assertTrue(list.get(0)==list.get(3));
	}
	
	@Test
	public void testConnectionPoolRecylesMany() throws SQLException {
		Database db = DatabaseCreator.create(2);
		TestSubscriber<Connection> ts = db.connections() //
				.test(4); //
		List<Connection> list = new ArrayList<>(ts.values());
		ts.assertValueCount(2) //
				.assertNotTerminated();
		list.get(1).close();
		ts.assertValueCount(3).assertNotTerminated();
		ts.assertValues(list.get(0), list.get(1), list.get(1));
		list.get(0).close();
		ts.assertValues(list.get(0), list.get(1), list.get(1), list.get(0));
		ts.assertValueCount(4).assertNotTerminated();
	}

}
