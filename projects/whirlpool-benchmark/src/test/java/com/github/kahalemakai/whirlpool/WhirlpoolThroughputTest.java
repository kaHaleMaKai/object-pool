package com.github.kahalemakai.whirlpool;

import lombok.val;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;

public class WhirlpoolThroughputTest {

    private int numSlowThreads = 5;
    private int slowCycles = 500;

    private int numFastThreads = 25;
    private int fastCycles = 10_000_000;

    @Test
    public void commonsPool() throws Exception {
        val pool = new GenericObjectPool<Sleeper>(new Sleeper.SleeperFactory());
        pool.setTestOnBorrow(true);
        pool.setTestOnReturn(true);
        val threads = new ArrayList<Thread>();
        val startSignal = new CountDownLatch(1);
        val stopSignal = new CountDownLatch(numSlowThreads);
        for (int i = 0; i < numSlowThreads; ++i) {
            val t = new Thread(() -> {
                try {
                    startSignal.await();
                } catch (InterruptedException e) {
                    throw new NullPointerException();
                }
                for (int j = 0; j < slowCycles; ++j) {
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
        val stopSignal = new CountDownLatch(numSlowThreads);
        for (int i = 0; i < numSlowThreads; ++i) {
            val t = new Thread(() -> {
                try {
                    startSignal.await();
                } catch (InterruptedException e) {
                    throw new NullPointerException();
                }
                for (int j = 0; j < slowCycles; ++j) {
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

    @Test
    public void whirlpoolThroughput() throws Exception {
        Thread.sleep(5000);
        val pool = Whirlpool.<Mutator<Long>>builder()
                .onCreate(() -> new Mutator<>(Thread.currentThread().getId()))
//                .onValidation(m -> (m.blockAndGet() + System.currentTimeMillis()) % 5 != 0)
                .onPrepare(m -> m.set(Thread.currentThread().getId()))
                .onReset(m -> m.set(0L))
                .onClose(m -> m.set(-1L))
                .expirationTime(1000)
//                .parallelism(1)
//                .asyncUnhand(false)
//                .asyncClose(false)
//                .asyncCreate(false)
//                .asyncFill(false)
                .build();
        val threads = new ArrayList<Thread>();
        val startSignal = new CountDownLatch(1);
        val stopSignal = new CountDownLatch(numFastThreads);
        for (int i = 0; i < numFastThreads; ++i) {
            val t = new Thread(() -> {
                try {
                    startSignal.await();
                } catch (InterruptedException e) {
                    throw new NullPointerException();
                }
                for (int j = 0; j < fastCycles; ++j) {
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
        System.out.println("size: " + pool.totalSize());
        System.out.println((diff));
        System.out.println(fastCycles * numFastThreads + " ops");
        System.out.println(fastCycles * numFastThreads / diff + " ops/ms");
    }


    @Test
    public void commonsPoolThroughput() throws Exception {
//        Thread.sleep(5000);
        val pool = new GenericObjectPool<Mutator<Long>>(new MutatorFactory());
//        pool.setTestOnReturn(true);
//        pool.setTestOnBorrow(true);
        pool.setMinEvictableIdleTimeMillis(1000);
        pool.setTimeBetweenEvictionRunsMillis(1000);
        val threads = new ArrayList<Thread>();
        val startSignal = new CountDownLatch(1);
        val stopSignal = new CountDownLatch(numFastThreads);
        for (int i = 0; i < numFastThreads; ++i) {
            val t = new Thread(() -> {
                try {
                    startSignal.await();
                } catch (InterruptedException e) {
                    throw new NullPointerException();
                }
                for (int j = 0; j < fastCycles; ++j) {
                    try {
                        pool.returnObject(pool.borrowObject());
                    } catch (Exception e) {
                        val ex = new NullPointerException();
                        ex.initCause(e);
                        throw ex;
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
        System.out.println(fastCycles * numFastThreads + " ops");
        System.out.println(fastCycles * numFastThreads / diff + " ops/ms");
    }

}