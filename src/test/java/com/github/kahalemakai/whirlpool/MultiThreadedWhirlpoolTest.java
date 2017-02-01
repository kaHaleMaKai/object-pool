package com.github.kahalemakai.whirlpool;

import lombok.val;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

public class MultiThreadedWhirlpoolTest {

    private Whirlpool<Integer> pool;
    private final long expirationTime = 1000;
    private volatile int counter;

    @Test
    public void borrowing() throws Exception {
        val ints = Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>());
        val threads = new ArrayList<Thread>();
        val numThreads = 4;
        val numCycles = 1000000;
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
        Assert.assertEquals(numCycles * numThreads, ints.size());
    }

    @Before
    public void setUp() throws Exception {
        counter = 0;
        pool = Whirlpool.<Integer>builder()
                .onCreate(() -> counter++)
                .expirationTime(expirationTime)
                .onClose(t -> counter--)
                .build();
    }

}