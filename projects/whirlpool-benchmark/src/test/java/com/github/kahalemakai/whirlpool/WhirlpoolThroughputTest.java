package com.github.kahalemakai.whirlpool;

import lombok.val;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;

public class WhirlpoolThroughputTest {

    private int numThreads = 1;
    private int cycles = 1000;

    @Test
    public void commonsPool() throws Exception {
        val pool = new GenericObjectPool<Sleeper>(new Sleeper.SleeperFactory());
        pool.setTestOnBorrow(true);
        pool.setTestOnReturn(true);
        val threads = new ArrayList<Thread>();
        val startSignal = new CountDownLatch(1);
        val stopSignal = new CountDownLatch(numThreads);
        for (int i = 0; i < numThreads; ++i) {
            val t = new Thread(() -> {
                try {
                    startSignal.await();
                } catch (InterruptedException e) {
                    throw new NullPointerException();
                }
                for (int j = 0; j < cycles; ++j) {
                    try {
                        pool.returnObject(pool.borrowObject());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    Thread.yield();
                }
                stopSignal.countDown();
            });
            t.start();
            threads.add(t);
        }
        val start = System.currentTimeMillis();
        startSignal.countDown();
        stopSignal.await();
        val end = System.currentTimeMillis();
        val diff = (end - start);
        System.out.println((diff));
    }

    @Test
    public void whirlpool() throws Exception {
        val pool = Whirlpool.<Sleeper>builder()
                .onCreate(Sleeper::new)
                .onClose(Sleeper::close)
                .onValidation(Sleeper::validate)
                .onPrepare(Sleeper::prepare)
                .onReset(Sleeper::reset)
                .expirationTime(30000)
                .parallelism(2)
                .asyncUnhand(false)
                .asyncClose(true)
                .asyncCreate(true)
                .asyncFill(false)
                .build();
        val threads = new ArrayList<Thread>();
        val startSignal = new CountDownLatch(1);
        val stopSignal = new CountDownLatch(numThreads);
        for (int i = 0; i < numThreads; ++i) {
            val t = new Thread(() -> {
                try {
                    startSignal.await();
                } catch (InterruptedException e) {
                    throw new NullPointerException();
                }
                for (int j = 0; j < cycles; ++j) {
                    pool.unhand(pool.borrow());
                    Thread.yield();
                }
                stopSignal.countDown();
            });
            t.start();
            threads.add(t);
        }
        val start = System.currentTimeMillis();
        startSignal.countDown();
        stopSignal.await();
        val end = System.currentTimeMillis();
        val diff = (end - start);
        System.out.println((diff));
    }

}