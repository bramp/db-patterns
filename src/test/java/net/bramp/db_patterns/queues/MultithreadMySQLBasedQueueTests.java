package net.bramp.db_patterns.queues;

import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.sql.DataSource;

import net.bramp.db_patterns.DatabaseUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultithreadMySQLBasedQueueTests {

	final static Logger LOG = LoggerFactory.getLogger(MultithreadMySQLBasedQueueTests.class);

	private String queueName;
	private DataSource ds;
	private ExecutorService executor;
	private MySQLBasedQueue<String> queue;

	public static class ConsumerCallable<V> implements Callable<V> {
		final BlockingQueue<V> queue;

		public ConsumerCallable(BlockingQueue<V> queue) {
			this.queue = queue;
		}

		public V call() throws Exception {
			LOG.info("Blocking");
			V value = queue.take();
			LOG.info("Took {}", value);

			return value;
		}
	}

	@Before
	public void setup() {
		// Different queue name for each test (to avoid test clashes)
		queueName = java.util.UUID.randomUUID().toString();
		ds = DatabaseUtils.createDataSource();

		queue = new MySQLBasedQueue<String>(ds, queueName, String.class);

		executor = Executors.newCachedThreadPool();
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
	
	@Test(timeout=1000)
	public void test() throws InterruptedException, ExecutionException, TimeoutException {
		assertEmpty();

		Callable<String> thread = new ConsumerCallable<String>(queue);
		Future<String> future = executor.submit(thread);
		
		Thread.sleep(100);
		assertFalse("Empty queue should be blocked", future.isDone());

		assertTrue( queue.add("A") );

		// It should unblock and consume almost instantly
		String ret = future.get(100, TimeUnit.MILLISECONDS);

		assertEquals("Queue head should be A", "A", ret);
		assertEmpty();
	}
}
