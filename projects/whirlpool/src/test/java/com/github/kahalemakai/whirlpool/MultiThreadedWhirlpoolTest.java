package com.github.kahalemakai.whirlpool;

import lombok.val;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

public class MultiThreadedWhirlpoolTest {

    private static final Method TRIGGER_EVICTION;
    static {
        Method m = null;
        try {
            m = Whirlpool.class.getDeclaredMethod("triggerEviction");
            m.setAccessible(true);
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        }
        TRIGGER_EVICTION = m;
    }

    private Whirlpool<Integer> pool;
    private final long expirationTime = 1000;
    private AtomicInteger counter;
    private final int cycles = 100000;

    @Test
    public void bgEviction() throws Exception {
        Logger.getLogger(AbstractObjectPool.class).setLevel(Level.INFO);
        Logger.getLogger(Whirlpool.class).setLevel(Level.INFO);
        pool.unhand(pool.borrow());
        assertEquals(1, pool.totalSize());
        TRIGGER_EVICTION.invoke(pool);
        Thread.sleep(2 * expirationTime);
        assertEquals(0, pool.totalSize());
        val list = new ArrayList<Integer>();
        for (int i = 0; i < cycles; ++i) {
            list.add(pool.borrow());
        }
        for (int i = 0; i < cycles; ++i) {
            pool.unhand(list.remove(0));
        }
        assertEquals(cycles, pool.availableElements());
        TRIGGER_EVICTION.invoke(pool);
        Thread.sleep(expirationTime * 2);
        assertEquals(0, pool.availableElements());
    }

    @Test
    public void borrowing() throws Exception {
        Logger.getLogger(Whirlpool.class).setLevel(Level.INFO);
        Logger.getLogger(AbstractObjectPool.class).setLevel(Level.INFO);
        Logger.getLogger(AbstractObjectPool.class).setLevel(Level.INFO);
        Thread.sleep(5000);
        val ints = Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>());
        val threads = new ArrayList<Thread>();
        val numThreads = 4;
        val numCycles = 1_000_000;
        val startSignal = new CountDownLatch(1);
        val endSignal = new CountDownLatch(numThreads);
        for (int i = 0; i < numThreads; ++i) {
            val thread = new Thread(() -> {
                try {
                    startSignal.await();
                } catch (InterruptedException e) {
                    endSignal.countDown();
                    Thread.currentThread().interrupt();
                    throw new NullPointerException();
                }
                for (int j = 0; j < numCycles; ++j) {
                    ints.add(pool.borrow());
                }
                endSignal.countDown();
            });
            threads.add(thread);
            thread.start();
        }
        startSignal.countDown();
        endSignal.await();
        assertEquals(numCycles * numThreads, ints.size());
    }

    @Before
    public void setUp() throws Exception {
        counter = new AtomicInteger(0);
        pool = Whirlpool.<Integer>builder()
                .onCreate(() -> counter.getAndIncrement())
                .expirationTime(expirationTime)
                .onClose(t -> counter.getAndDecrement())
                .asyncClose(true)
                .asyncUnhand(false)
                .asyncCreate(true)
                .parallelism(4)
                .build();
    }

}