package net.bramp.db_patterns.queues;

import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import net.bramp.concurrent.ConcurrentBitSet;
import net.bramp.db_patterns.DatabaseUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stress tests
 * @author bramp
 *
 */
public class StressMySQLBasedQueueTests {

	final static Logger LOG = LoggerFactory.getLogger(StressMySQLBasedQueueTests.class);

	private String queueName;
	private DataSource ds;
    private String me;
	private ExecutorService executor;
	private MySQLBasedQueue<Integer> queue;

	public static class ProducerCallable implements Callable<Void> {
		final BlockingQueue<Integer> queue;
		final ConcurrentBitSet set;

		final int interval;
		final int end;
		int current;

		public ProducerCallable(BlockingQueue<Integer> queue, ConcurrentBitSet set, int start, int interval, int end) {
			this.queue = queue;
			this.set = set;
			this.current = start;
			this.interval = interval;
			this.end = end;
		}

		public Void call() throws Exception {

			while (current < end) {
				LOG.info("Adding " + current);
				assertFalse(set.get(current));
				queue.add(current);
				current += interval;
			}

			return null;
		}
	}

	public static class ConsumerCallable implements Callable<Void> {
		final BlockingQueue<Integer> queue;
		final ConcurrentBitSet set;
		final int end;

		public ConsumerCallable(BlockingQueue<Integer> queue, ConcurrentBitSet set, int end) {
			this.queue = queue;
			this.set = set;
			this.end = end;
		}

		public Void call() throws Exception {

			while (true) {
				Integer value = queue.poll(5, TimeUnit.SECONDS);
				if (value == null)
					break;
				LOG.info("Took " + value);
				if (!set.compareAndSet(value, false, true))
					fail("Bit is already set " + value);
			}

			LOG.info("Nothing more to consume!");
			return null;
		}
	}


	@Before
	public void setup() {
		// Different queue name for each test (to avoid test clashes)
		queueName = java.util.UUID.randomUUID().toString();
		ds = DatabaseUtils.createDataSource();
        me = DatabaseUtils.getHostname();

		queue = new MySQLBasedQueue<Integer>(ds, queueName, Integer.class, me);

		executor = Executors.newCachedThreadPool();
	}

	@After
	public void cleanupDatabase() throws SQLException {
		queue.clear();
		queue.cleanupAll();
		assertEmpty();
	}
	
	protected void assertEmpty() {
		assertTrue("Queue should be empty", queue.isEmpty());
		assertEquals("Queue should be empty", 0, queue.size());
		assertNull("Queue head should be null", queue.peek());
	}
	
	/**
	 * Multiple consumers and producers
	 * @throws InterruptedException 
	 */
	@Test(timeout=120000)
	public void stress() throws InterruptedException {
		assertEmpty();

		final int producers = 8;
		final int consumers = 8;
		final int iterations = 100;

		ConcurrentBitSet bitset = new ConcurrentBitSet(iterations);

		for (int i = 0; i < producers; i++) {
			executor.submit( new ProducerCallable(queue, bitset, i, producers, iterations) );
		}

		for (int i = 0; i < consumers; i++) {
			executor.submit( new ConsumerCallable(queue, bitset, iterations) );
		}

		// Tell the executor to accept no more requests
		executor.shutdown();

		int lastCardinality = bitset.cardinality();
		while (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
			// This tests if work is still happening - Wait as long as it takes
			int newCardinality = bitset.cardinality();
			if (newCardinality == lastCardinality) {
				LOG.info("Missing " + bitset.getClearBits(iterations));
				fail("The stress test got stuck at " + lastCardinality);
			}
			lastCardinality = newCardinality;
		}

		assertEmpty();
	}
}
