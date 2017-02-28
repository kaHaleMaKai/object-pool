package com.github.kahalemakai.whirlpool;

import lombok.val;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RingBufferTest {

    @Test
    public void size() throws Exception {
        val maxSize = 100;
        final RingBuffer<Integer> buffer = new RingBuffer<>(Integer.class, maxSize);
        for (int i = 0; i < maxSize; ++i) {
            buffer.offer(i);
            assertEquals(i + 1, buffer.availableElements());
        }
        for (int i = 0; i < maxSize; ++i) {
            int el = buffer.poll();
            assertEquals(i, el);
            assertEquals(maxSize - 1 - i, buffer.availableElements());
        }
    }
//
//    @Test
//    public void offerFails() throws Exception {
//        final RingBuffer<Integer> buffer = new RingBuffer<>(Integer.class, 1);
//        assertTrue(buffer.offer(0));
//        assertFalse(buffer.offer(1));
//    }
//
//    @Test
//    public void pollFails() throws Exception {
//        final RingBuffer<Integer> buffer = new RingBuffer<>(Integer.class, 0);
//        assertNull(buffer.poll());
//    }
//
//    @Test
//    public void normalize() throws Exception {
//        final RingBuffer<Integer> buffer = new RingBuffer<>(Integer.class, 17);
//        val initialSteps = 3;
//        val steps = buffer.getMaxSize() * 5 - 3;
//        for (int i = 0; i < initialSteps; ++i) {
//            buffer.offer(i);
//        }
//        for (int i = 0; i < steps; ++i) {
//            buffer.offer(i);
//            buffer.poll();
//        }
//        assertEquals(3, buffer.availableElements());
//        buffer.normalizeHeadAndTail();
//        val field = RingBuffer.class.getDeclaredField("headAndTail");
//        field.setAccessible(true);
//        final long headAndTail = field.getLong(buffer);
//        val head = getHead(headAndTail);
//        val tail = getTail(headAndTail);
//        assertEquals(steps % buffer.getMaxSize(), head);
//        assertEquals((steps + initialSteps) % buffer.getMaxSize(), tail);
//        assertEquals(3, buffer.availableElements());
//    }
//
//    private final int numThreads = 5;
//    private final int cycles = 1_000_000;
//
//    @Test
//    public void performance() throws Exception {
////        Thread.sleep(5000);
//        val maxSize = 32;
//        final RingBuffer<Long> buffer = new RingBuffer<>(Long.class, maxSize);
////        val executors = Executors.newFixedThreadPool(numThreads);
//        val startSignal = new CountDownLatch(1);
//        val endSignal = new CountDownLatch(numThreads);
//        for (int i = 0; i < numThreads; ++i) {
////            executors.submit(() -> {
//            new Thread(() -> {
//                try {
//                    buffer.offer(Thread.currentThread().getId());
//                    startSignal.await();
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                    throw new NullPointerException();
//                }
//                for (int j = 0; j < cycles; j++) {
//                    val result = buffer.poll();
//                    if (result == null) {
//                        endSignal.countDown();
//                        Thread.currentThread().interrupt();
//                        throw new NullPointerException(
//                                String.format("could not poll. thread: %d, cycle: %d",
//                                        Thread.currentThread().getId(), j));
//
//                    }
//                    val success = buffer.offer(result);
//                    if (!success) {
//                        endSignal.countDown();
//                        Thread.currentThread().interrupt();
//                        throw new NullPointerException(
//                                String.format("could not offer. thread: %d, cycle: %d",
//                                        Thread.currentThread().getId(), j));
//                    }
//                    Thread.yield();
//                }
//                endSignal.countDown();
////            });
//            }).start();
//        }
//        startSignal.countDown();
//        endSignal.await();
////        executors.shutdown();
//    }

}