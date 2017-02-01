package com.github.kahalemakai.whirlpool;

import lombok.val;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class SingleThreadedWhirlpoolTest {
    private Whirlpool<Integer> pool;
    private final long expirationTime = 1000;
    private volatile int counter;

    @Test
    public void evict() throws Exception {
        val borrowed = pool.borrow();
        assertEquals(Integer.valueOf(1), Integer.valueOf(pool.elementsCreated()));
        Thread.sleep(expirationTime);
        pool.evict(borrowed);
        assertEquals(Integer.valueOf(1), Integer.valueOf(pool.elementsCreated()));
        pool.unhand(borrowed);
        pool.evict(borrowed);
        assertEquals(Integer.valueOf(1), Integer.valueOf(pool.elementsCreated()));
        Thread.sleep(expirationTime);
        pool.evict(borrowed);
        assertEquals(Integer.valueOf(0), Integer.valueOf(pool.elementsCreated()));
        assertEquals(borrowed, pool.borrow());
    }

    @Test
    public void remove() throws Exception {
        val borrowed = pool.borrow();
        assertEquals(Integer.valueOf(1), Integer.valueOf(pool.elementsCreated()));
        pool.removeNow(borrowed);
        assertEquals(Integer.valueOf(1), Integer.valueOf(pool.elementsCreated()));
        pool.unhand(borrowed);
        pool.removeNow(borrowed);
        assertEquals(Integer.valueOf(0), Integer.valueOf(pool.elementsCreated()));
        assertEquals(borrowed, pool.borrow());
    }

    @Test
    public void evictAll() throws Exception {
        val borrowed = pool.borrow();
        assertEquals(Integer.valueOf(1), Integer.valueOf(pool.elementsCreated()));
        Thread.sleep(expirationTime);
        pool.evictAll();
        assertEquals(Integer.valueOf(1), Integer.valueOf(pool.elementsCreated()));
        pool.unhand(borrowed);
        pool.evictAll();
        assertEquals(Integer.valueOf(1), Integer.valueOf(pool.elementsCreated()));
        Thread.sleep(expirationTime);
        pool.evictAll();
        assertEquals(Integer.valueOf(0), Integer.valueOf(pool.elementsCreated()));
        assertEquals(borrowed, pool.borrow());
    }

    @Test
    public void sizes() throws Exception {
        val list = new ArrayList<Integer>();
        list.add(pool.borrow());
        list.add(pool.borrow());
        list.add(pool.borrow());
        list.add(pool.borrow());
        pool.unhand(list.get(3));
        assertEquals(Integer.valueOf(4), Integer.valueOf(pool.elementsCreated()));
        assertEquals(Integer.valueOf(4), Integer.valueOf(pool.elementsCreatedEstimate()));
        assertEquals(Integer.valueOf(1), Integer.valueOf(pool.availableElements()));
        assertEquals(Integer.valueOf(1), Integer.valueOf(pool.availableElementsEstimate()));
    }

    @Test
    public void borrowing() throws Exception {
        assertEquals(Integer.valueOf(0), pool.borrow());
        assertEquals(Integer.valueOf(1), pool.borrow());
        assertEquals(Integer.valueOf(2), pool.borrow());
        val borrowed = pool.borrow();
        assertEquals(Integer.valueOf(3), borrowed);
        pool.unhand(borrowed);
        assertEquals(Integer.valueOf(3), pool.borrow());
    }

    @Test
    public void validation() throws Exception {
        final int maxAllowedInt = 5;
        val intPool = Whirlpool.<Integer>builder()
                .expirationTime(expirationTime)
                .onCreate(() -> counter++)
                .onClose(t -> counter = 0)
                .onValidation(t -> t < maxAllowedInt)
                .build();
        val list = new ArrayList<Integer>();
        for (int i = 0; i <= maxAllowedInt + 1; ++i) {
            list.add(intPool.borrow());
            assertEquals(Integer.valueOf(i), list.get(i));
        }
        intPool.unhand(list.get(maxAllowedInt + 1));
        assertEquals(Integer.valueOf(0), intPool.borrow());
    }

    @Test
    public void expirationTime() throws Exception {
        assertEquals(expirationTime, pool.getExpirationTime());
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