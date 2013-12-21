package net.bramp.concurrent;

import static org.junit.Assert.*;

import java.util.Set;

import net.bramp.concurrent.ConcurrentBitSet;

import org.junit.Test;

public class ConcurrentBitSetTest {

	@Test
	public void testGetClearBits() {
		ConcurrentBitSet bitset = new ConcurrentBitSet(16);
		Set<Integer> clearBits = bitset.getClearBits(16);

		assertEquals(16, clearBits.size());
		for (int i = 0; i < 16; i++) {
			assertTrue(clearBits.contains(i));
		}
	}

}
