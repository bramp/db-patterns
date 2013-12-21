package net.bramp.db_patterns.queues;

import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import net.bramp.db_patterns.DatabaseUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MySQLBasedQueueTests {

	final static long WAIT_FOR_TIMING_TEST = 300; // in ms
	
	private String queueName;
	private DataSource ds;
	
	private MySQLBasedQueue<String> queue;

	@Before
	public void setup() {
		// Different queue name for each test (to avoid test clashes)
		queueName = java.util.UUID.randomUUID().toString();
		ds = DatabaseUtils.createDataSource();

		queue = new MySQLBasedQueue<String>(ds, queueName, String.class);
	}

	@After
	public void cleanupDatabase() throws SQLException {
		queue.clear();
		queue.cleanupAll();
		assertEmpty();
	}
	
	protected void assertEmpty() {
		assertTrue("Queue should start empty", queue.isEmpty());
		assertEquals("Queue should start empty", 0, queue.size());
		assertNull("Queue head should be null", queue.peek());
	}
	
	@Test
	public void test() {
		assertEmpty();

		assertTrue( queue.add("A") );

		assertEquals("Queue should contain one item", 1, queue.size());
		assertEquals("Queue head should be A", "A", queue.peek());

		assertTrue( queue.add("B") );

		assertEquals("Queue should start empty", 2, queue.size());
		assertEquals("Queue head should be A", "A", queue.peek());

		assertEquals("Queue head should be A", "A", queue.poll());

		assertEquals("Queue should start empty", 1, queue.size());
		assertEquals("Queue head should be B", "B", queue.peek());
		
		assertEquals("Queue head should be B", "B", queue.poll());

		assertEmpty();
	}

	/* TODO We should change this to measure if take actually blocked forever
	@Test(timeout=5000)
	public void takeBlockingTest() throws InterruptedException {
		assertEmpty();
		
		// This should block forever
		queue.take();
		
		assertEmpty();
	}
	*/
	
	@Test(timeout=1000)
	public void pollBlockingTest() throws InterruptedException {
		assertEmpty();

		long wait = WAIT_FOR_TIMING_TEST;

		long now = System.currentTimeMillis();
		String ret = queue.poll(wait, TimeUnit.MILLISECONDS);
		long duration = System.currentTimeMillis() - now;

		assertNull("poll timed out", ret);

		assertTrue("We waited less than " + wait + "ms (actual:" + duration + ")", duration >= wait);
		assertTrue("We waited more than " + (wait*1.2) + "ms (actual:" + duration + ")", duration < wait * 1.2);

		assertEmpty();
	}
}
