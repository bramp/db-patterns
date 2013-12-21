package net.bramp.db_patterns.queues;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * To keep code neat, most of the simple methods are here
 * @author bramp
 *
 */
public abstract class AbstractBlockingQueue<E> implements BlockingQueue<E> {

	public boolean isEmpty() {
		return size() == 0;
	}

	public boolean offer(E e) {
		return add(e);
	}
	
	public E element() {
		E head = peek();
		if (head == null)
			throw new NoSuchElementException();
		return head;
	}

	public E remove() {
		E head = poll();
		if (head == null)
			throw new NoSuchElementException();
		return head;
	}

	/**
	 * Blocks
	 */
	public E take() throws InterruptedException {
		// We loop around trying to get a item, blocking at most a minute at
		// a time this allows us to be interrupted
		E head = null;
		while (head == null) {
			if (Thread.interrupted())
				throw new InterruptedException();

			head = poll(1, TimeUnit.MINUTES);
		}
		return head;
	}

	/**
	 * Blocks
	 */
	public void put(E e) throws InterruptedException {
		add(e);
	}
	
	/**
	 * No blocking
	 */
	public int drainTo(Collection<? super E> c) {
		return drainTo(c, Integer.MAX_VALUE);
	}

	/**
	 * No blocking
	 */
	public int drainTo(Collection<? super E> c, int maxElements) {
		if (c == this)
			throw new IllegalArgumentException("Draining to self is not supported");

		int count = 0;
		while (count < maxElements) {
			E head = poll();
			if (head == null)
				break;

			c.add(head);
			count++;
		}

		return maxElements - count;
	}

	public void clear() {
		// Lazy! just keep poll'ng them off
		while (poll() != null) {
			// Nothing
		}
	}
	
	public int remainingCapacity() {
		return Integer.MAX_VALUE;
	}

	public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
		// Right now, we have no concept of a full queue, so we don't block on insert
		return offer(e);
	}

	////// Nothing supported below

	public boolean contains(Object o) {
		throw new UnsupportedOperationException();
	}

	public Iterator<E> iterator() {
		throw new UnsupportedOperationException();
	}

	public Object[] toArray() {
		throw new UnsupportedOperationException();	}

	public <T> T[] toArray(T[] a) {
		throw new UnsupportedOperationException();
	}

	public boolean remove(Object o) {
		throw new UnsupportedOperationException();
	}

	public boolean containsAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	public boolean addAll(Collection<? extends E> c) {
		throw new UnsupportedOperationException();
	}

	public boolean removeAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	public boolean retainAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}
}
