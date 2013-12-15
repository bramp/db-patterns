package net.bramp.db_patterns;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;

import javax.sql.DataSource;

import net.bramp.concurrent.Futures;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

/**
 * TODO Test it doesn't interfere with other locks
 * @author bramp
 *
 */
public class MySQLSleepBasedConditionTest {

	final static Logger LOG = LoggerFactory.getLogger(MySQLSleepBasedConditionTest.class);
	
	ExecutorService executor;
	
	DataSource ds;
	MySQLSleepBasedCondition condition;

	AtomicBoolean shouldBeASleep;
	AtomicInteger numberAwake;

	List<Future<Void>> futures;

	@Before
	public void setup() throws InterruptedException {

		ds = DatabaseUtils.createDataSource();
		condition = new MySQLSleepBasedCondition(ds, "lock");

		executor = Executors.newCachedThreadPool();

		shouldBeASleep = new AtomicBoolean(true);
		numberAwake = new AtomicInteger(0);
		
		// Create three threads
		Callable<Void> awaitCallable = new AwaitCallable(shouldBeASleep, numberAwake, condition);

		futures = new ArrayList<Future<Void>>();
		futures.add( executor.submit(awaitCallable) );
		futures.add( executor.submit(awaitCallable) );
		futures.add( executor.submit(awaitCallable) );

		Thread.sleep(100);
	}

	@After
	public void tearDown() {
		executor.shutdownNow();
	}

	public static class AwaitCallable implements Callable<Void> {
		final AtomicBoolean shouldBeASleep;
		final AtomicInteger numberAwake;
		final Condition condition;
		
		public AwaitCallable(final AtomicBoolean shouldBeASleep, AtomicInteger numberAwake, Condition condition) {
			this.shouldBeASleep = shouldBeASleep;
			this.numberAwake = numberAwake;
			this.condition = condition;
		}

		public Void call() {
			try {
				assertTrue("Thread hasn't had a chance to await", shouldBeASleep.get());

				LOG.info("Blocking");
				condition.await();
				numberAwake.incrementAndGet();
				LOG.info("Awake");

				assertFalse("Thread woke up too early", shouldBeASleep.get());

			} catch (Throwable t) {
				throw Throwables.propagate(t);
			}

			return null;
		}
	}

	protected void waitForAllFutures() throws InterruptedException, ExecutionException {
		try {
			Futures.getAll(futures, 200, TimeUnit.MILLISECONDS);

		} catch (TimeoutException e) {
			// Ignore Timeout as that's ok
		}
	}
	
	/**
	 * We sleep three threads, and notify none to awake
	 * @throws InterruptedException
	 * @throws ExecutionException
	 * @throws TimeoutException
	 */
	@Test(timeout = 1000)
	public void testAwaitSignalNone() throws InterruptedException, ExecutionException, TimeoutException {
		waitForAllFutures();
		assertEquals("Expected only one thread to wake", 0, numberAwake.get());
	}
	
	/**
	 * We sleep three threads, and notify only one to awake
	 * @throws InterruptedException
	 * @throws ExecutionException
	 * @throws TimeoutException
	 */
	@Test(timeout = 1000)
	public void testAwaitSignalOne() throws InterruptedException, ExecutionException, TimeoutException {
		// Now wake it up
		LOG.info("Signal");
		shouldBeASleep.set(false);
		condition.signal();

		waitForAllFutures();
		assertEquals("Expected only one thread to wake", 1, numberAwake.get());
	}
	
	/**
	 * We sleep three threads, and notify only one to awake
	 * @throws InterruptedException
	 * @throws ExecutionException
	 * @throws TimeoutException
	 */
	@Test(timeout = 1000)
	public void testAwaitSignalAll() throws InterruptedException, ExecutionException, TimeoutException {
		// Now wake it up
		LOG.info("SignalAll");
		shouldBeASleep.set(false);
		condition.signalAll();

		waitForAllFutures();
		assertEquals("Expected only one thread to wake", 3, numberAwake.get());
	}
}
