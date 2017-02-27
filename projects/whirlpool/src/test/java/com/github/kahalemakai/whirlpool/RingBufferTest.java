package com.github.kahalemakai.whirlpool;

import lombok.val;
import org.junit.Test;

import static org.junit.Assert.*;

public class RingBufferTest {

    @Test
    public void size() throws Exception {
        val maxSize = 100;
        final RingBuffer<Integer> buffer = new RingBuffer<>(Integer.class, maxSize);
        for (int i = 0; i < maxSize; ++i) {
            assertEquals(i, buffer.availableElements());
            assertTrue(buffer.offer(i));
        }
        for (int i = 0; i < maxSize; ++i) {
            int el = buffer.poll();
            assertEquals(i, el);
            assertEquals(maxSize - 1 - i, buffer.availableElements());
        }
    }

    @Test
    public void offerFails() throws Exception {
        final RingBuffer<Integer> buffer = new RingBuffer<>(Integer.class, 1);
        assertTrue(buffer.offer(0));
        assertFalse(buffer.offer(1));
    }

    @Test
    public void pollFails() throws Exception {
        final RingBuffer<Integer> buffer = new RingBuffer<>(Integer.class, 0);
        assertNull(buffer.poll());
    }

    @Test
    public void normalize() throws Exception {
        final RingBuffer<Integer> buffer = new RingBuffer<>(Integer.class, 17);
        val initialSteps = 3;
        val steps = buffer.getMaxSize() * 5 - 3;
        for (int i = 0; i < initialSteps; ++i) {
            buffer.offer(i);
        }
        for (int i = 0; i < steps; ++i) {
            buffer.offer(i);
            buffer.poll();
        }
        assertEquals(3, buffer.availableElements());
        buffer.normalizeHeadAndTail();
        val field = RingBuffer.class.getDeclaredField("headAndTail");
        field.setAccessible(true);
        final long headAndTail = field.getLong(buffer);
        val head = getHead(headAndTail);
        val tail = getTail(headAndTail);
        assertEquals(steps % buffer.getMaxSize(), head);
        assertEquals((steps + initialSteps) % buffer.getMaxSize(), tail);
        assertEquals(3, buffer.availableElements());
    }

    static int getHead(long headAndTail) {
        return ((int) (headAndTail >>> 32));
    }

    static int getTail(long headAndTail) {
        return (int) headAndTail;
    }

}