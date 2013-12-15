package net.bramp.concurrent;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class Futures {
	private Futures() {}

	/**
	 * Calls get() on all futures waiting for responses If any future throws an
	 * Exception, this method throws it (without returning the list of
	 * successful responses)
	 * 
	 * @param futures
	 * @return
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public static <V> List<V> getAll(List<Future<V>> futures)
			throws InterruptedException, ExecutionException {
		List<V> results = new ArrayList<V>(futures.size());
		for (Future<V> future : futures) {
			results.add(future.get());
		}
		return Collections.unmodifiableList(results);
	}

	/**
	 * Calls get() on all futures waiting for responses until the timeout If any
	 * future throws an Exception, this method throws it (without returning the
	 * list of successful responses)
	 * 
	 * @param futures
	 * @param timeout
	 * @param unit
	 * @return
	 * @throws InterruptedException
	 * @throws ExecutionException
	 * @throws TimeoutException
	 */
	@SuppressWarnings("unchecked")
	public static <V> List<V> getAll(List<Future<V>> futures, long timeout,
			TimeUnit unit) throws InterruptedException, ExecutionException,
			TimeoutException {

		long timeoutNS = unit.toNanos(timeout);
		long deadline = System.nanoTime() + timeoutNS;

		// Make a copy
		futures = new LinkedList<Future<V>>(futures);

		final int size = futures.size();
		BitSet done = new BitSet(size);

		V[] results = (V[]) new Object[size];
		while (done.cardinality() < size) {

			for (int i = 0; i < size; i++) {
				// TODO - We could use done.nextClearBit(i)
				if (done.get(i))
					continue;

				Future<V> future = futures.get(i);
				try {
					// We wait just a fraction, to give everyone at least two chances
					results[i] = future.get(timeoutNS / (2 * size), TimeUnit.NANOSECONDS);
					done.set(i);

				} catch (ExecutionException e) {
					unwrapExecutionException(e);

				} catch (TimeoutException e) {
					// If we have exceeded our deadline, throw, otherwise
					if (System.nanoTime() >= deadline)
						throw e;
				}
			}
		}

		return Collections.unmodifiableList(Arrays.asList(results));
	}

	/**
	 * Sometimes InterruptedException is wrapped in an ExecutionException
	 * I don't think that's correct behavior, but lets fix it here
	 * @param e
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public static void unwrapExecutionException(ExecutionException e) throws InterruptedException, ExecutionException {
		if (e.getCause() instanceof InterruptedException)
			throw (InterruptedException)e.getCause();

		throw e;
	}
}
