package net.bramp.concurrent;

import java.util.BitSet;
import java.util.Set;
import java.util.TreeSet;

/**
 * Simple wrapper around BitSet to make it thread safe
 * @author bramp
 *
 */
public class ConcurrentBitSet {
	final BitSet set;

	public ConcurrentBitSet() {
		set = new BitSet();
	}
	
	public ConcurrentBitSet(int nbits) {
		set = new BitSet(nbits);
	}
	
	/**
	 * Atomically sets the value to the given updated value if the current value == the expected value.
	 * 
	 * @param bitIndex
	 * @param expect
	 * @param update
	 * @return true if successful. False return indicates that the actual value was not equal to the expected value.
	 */
	public synchronized boolean compareAndSet(int bitIndex, boolean expect, boolean update) {
		if (get(bitIndex) == expect) {
			set(bitIndex, update);
			return true;
		}
		return false;
	}
	
	/**
	 * Return a set of clear indexes
	 * @return
	 */
	public Set<Integer> getClearBits(int max) {
		Set<Integer> missing = new TreeSet<Integer>();
		int nextBit = -1;
		while (true) {
			nextBit = this.nextClearBit(nextBit + 1);
			if (nextBit >= max)
				break;
			missing.add(nextBit);
		}
		return missing;
	}

	public synchronized byte[] toByteArray() {
		return set.toByteArray();
	}

	public synchronized long[] toLongArray() {
		return set.toLongArray();
	}

	public synchronized void flip(int bitIndex) {
		set.flip(bitIndex);
	}

	public synchronized void flip(int fromIndex, int toIndex) {
		set.flip(fromIndex, toIndex);
	}

	public synchronized void set(int bitIndex) {
		set.set(bitIndex);
	}

	public synchronized void set(int bitIndex, boolean value) {
		set.set(bitIndex, value);
	}

	public synchronized void set(int fromIndex, int toIndex) {
		set.set(fromIndex, toIndex);
	}

	public synchronized void set(int fromIndex, int toIndex, boolean value) {
		set.set(fromIndex, toIndex, value);
	}

	public synchronized void clear(int bitIndex) {
		set.clear(bitIndex);
	}

	public synchronized void clear(int fromIndex, int toIndex) {
		set.clear(fromIndex, toIndex);
	}

	public synchronized void clear() {
		set.clear();
	}

	public synchronized boolean get(int bitIndex) {
		return set.get(bitIndex);
	}

	public synchronized BitSet get(int fromIndex, int toIndex) {
		return set.get(fromIndex, toIndex);
	}

	public synchronized int nextSetBit(int fromIndex) {
		return set.nextSetBit(fromIndex);
	}

	public synchronized int nextClearBit(int fromIndex) {
		return set.nextClearBit(fromIndex);
	}

	public synchronized int previousSetBit(int fromIndex) {
		return set.previousSetBit(fromIndex);
	}

	public synchronized int previousClearBit(int fromIndex) {
		return set.previousClearBit(fromIndex);
	}

	public synchronized int length() {
		return set.length();
	}

	public synchronized boolean isEmpty() {
		return set.isEmpty();
	}

	public synchronized boolean intersects(BitSet set) {
		return set.intersects(set);
	}

	public synchronized int cardinality() {
		return set.cardinality();
	}

	public synchronized void and(BitSet set) {
		set.and(set);
	}

	public synchronized void or(BitSet set) {
		set.or(set);
	}

	public synchronized void xor(BitSet set) {
		set.xor(set);
	}

	public synchronized void andNot(BitSet set) {
		set.andNot(set);
	}

	public synchronized int hashCode() {
		return set.hashCode();
	}

	public synchronized int size() {
		return set.size();
	}

	public synchronized boolean equals(Object obj) {
		if (obj instanceof ConcurrentBitSet)
			return set.equals( ((ConcurrentBitSet)obj).set );

		return false;
	}

	public synchronized String toString() {
		return set.toString();
	}
}