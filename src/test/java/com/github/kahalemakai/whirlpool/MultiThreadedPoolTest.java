package com.github.kahalemakai.whirlpool;

import lombok.val;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertEquals;

public class MultiThreadedPoolTest {

    private ObjectPool<Integer> pool;
    private final long expirationTime = 100;
    private volatile int counter;
    private final int cycles = 300;

    @Test
    public void bgEviction() throws Exception {
        pool.scheduleForEviction();
        pool.unhand(pool.borrow());
        assertEquals(1, pool.totalSize());
        Thread.sleep(expirationTime * 2);
        assertEquals(0, pool.totalSize());
        val list = new ArrayList<Integer>();
        for (int i = 0; i < cycles; ++i) {
            list.add(pool.borrow());
        }
        for (int i = 0; i < cycles; ++i) {
            pool.unhand(list.remove(0));
        }
        assertEquals(cycles, pool.totalSize());
        Thread.sleep(2 * expirationTime);
        assertEquals(0, pool.totalSize());
    }

    @Test
    public void borrowing() throws Exception {
        val ints = Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>());
        val threads = new ArrayList<Thread>();
        val numThreads = 4;
        val numCycles = 1000000;
        Logger.getLogger(AbstractObjectPool.class).setLevel(Level.INFO);
        Logger.getLogger(ObjectPool.class).setLevel(Level.INFO);
        for (int i = 0; i < numThreads; ++i) {
            val thread = new Thread(() -> {
                for (int j = 0; j < numCycles; ++j) {
                    ints.add(pool.borrow());
                }
            });
            threads.add(thread);
            thread.start();
        }
        while (threads.stream().anyMatch(Thread::isAlive)) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                threads.forEach(Thread::interrupt);
                throw e;
            }
        }
        assertEquals(numCycles * numThreads, ints.size());
    }

    @Before
    public void setUp() throws Exception {
        counter = 0;
        pool = ObjectPool.<Integer>builder()
                .onCreate(() -> counter++)
                .expirationTime(expirationTime)
                .onClose(t -> counter--)
                .build();
    }

}