package com.github.kahalemakai.whirlpool;

import lombok.val;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;

public class RingBufferTest {

    private final int numThreads = 50;
    private final int cycles = 1_000_000;

    @Test
    public void size() throws Exception {
        val maxSize = 100;
        final RingBuffer<Integer> buffer = new RingBuffer<>(maxSize);
        for (int i = 0; i < maxSize; ++i) {
            buffer.put(i);
            assertEquals(i + 1, buffer.size());
        }
        for (int i = 0; i < maxSize; ++i) {
            int el = buffer.take();
            assertEquals(i, el);
            assertEquals(maxSize - 1 - i, buffer.size());
        }
    }

    @Test
    public void performanceCLQ() throws Exception {
        val queue = new ConcurrentLinkedQueue<Integer>();
        val startSignal = new CountDownLatch(1);
        val endSignal = new CountDownLatch(numThreads);
        val values = new HashSet<Integer>();
        val counter = new AtomicLong();
        for (int i = 0; i < numThreads; ++i) {
            val idx = i;
            new Thread(() -> {
                try {
                    queue.add(idx);
                    startSignal.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new NullPointerException();
                }
                for (int j = 0; j < cycles; j++) {
                    val result = queue.poll();
                    values.add(result);
                    counter.getAndIncrement();
                    queue.add(result);
                    Thread.yield();
                }
                endSignal.countDown();
            }).start();
        }
        startSignal.countDown();
        endSignal.await();
        assertEquals(numThreads * cycles, counter.getAndIncrement());
        System.out.println(values);
        assertEquals(numThreads, values.size());
    }

    @Test
    public void performanceABQ() throws Exception {
        val maxSize = 64;
        val queue = new ArrayBlockingQueue<Integer>(maxSize, true);
        val startSignal = new CountDownLatch(1);
        val endSignal = new CountDownLatch(numThreads);
        val values = new HashSet<Integer>();
        val counter = new AtomicLong();
        for (int i = 0; i < numThreads; ++i) {
            val idx = i;
            new Thread(() -> {
                try {
                    queue.add(idx);
                    startSignal.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new NullPointerException();
                }
                for (int j = 0; j < cycles; j++) {
                    val result = queue.poll();
                    values.add(result);
                    counter.getAndIncrement();
                    queue.add(result);
                    Thread.yield();
                }
                endSignal.countDown();
            }).start();
        }
        startSignal.countDown();
        endSignal.await();
        assertEquals(numThreads * cycles, counter.getAndIncrement());
        assertEquals(numThreads, values.size());
    }

    @Test
    public void performance() throws Exception {
        val maxSize = 64;
        final RingBuffer<Integer> buffer = new RingBuffer<>(maxSize);
        val startSignal = new CountDownLatch(1);
        val endSignal = new CountDownLatch(numThreads);
        val values = Collections.newSetFromMap(new ConcurrentHashMap<>());
        val counter = new AtomicLong();
        for (int i = 0; i < numThreads; ++i) {
            val idx = i;
            val t = new Thread(() -> {
                try {
                    buffer.put(idx);
                    startSignal.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    throw new NullPointerException();
                }
                for (int j = 0; j < cycles; j++) {
                    val result = buffer.take();
                    values.add(result);
                    counter.getAndIncrement();
                    buffer.put(result);
                    Thread.yield();
                }
                endSignal.countDown();
            });
            t.setName("perf-test-"+i);
            t.start();
        }
        startSignal.countDown();
        endSignal.await();
        assertEquals(numThreads * cycles, counter.getAndIncrement());
        assertEquals(numThreads, values.size());
    }

}