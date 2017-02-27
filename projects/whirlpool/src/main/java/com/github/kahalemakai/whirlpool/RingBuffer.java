package com.github.kahalemakai.whirlpool;

import lombok.Getter;
import lombok.val;
import sun.misc.Unsafe;

import java.lang.reflect.Array;
import java.lang.reflect.Field;

public class RingBuffer<T> {

    private static final Unsafe UNSAFE;
    static {
        UNSAFE = initUnsafe();
    }

    private static final long HEAD_OFFSET;
    static {
        val offset = getHeadOffset();
        HEAD_OFFSET = offset;
    }

    private static final int INVALID_INDEX = -1;
    private static final long LONG_BITS = 0xffffffffL;

    private volatile long headAndTail;
    private final T[] buffer;
    @Getter
    private final int maxSize;

    RingBuffer(Class<? extends T> type, int maxSize) {
        this.maxSize = maxSize;
        @SuppressWarnings("unchecked")
        val array = (T[]) Array.newInstance(type, this.maxSize);
        this.buffer = array;
        this.headAndTail = 0L;
    }

    public boolean offer(T element) {
        val idx = getAndIncrementTail();
        if (idx == INVALID_INDEX) {
            return false;
        }
        buffer[idx] = element;
        return true;
    }

    public T poll() {
        val idx = getAndIncrementHead();
        if (idx == INVALID_INDEX) {
            return null;
        }
        val element = buffer[idx];
        buffer[idx] = null;
        return element;
    }

    public int availableElements() {
        val address = headAndTail;
        val h = ((int) (address >>> 32));
        val t = (int) address;
        val diff = (t - h + maxSize) % maxSize;
        if (diff == 0) {
            return t > h ? maxSize : 0;
        }
        return diff;
    }

    void normalizeHeadAndTail() {
        int h, t;
        long prev, next;
        do {
            prev = headAndTail;
            h = ((int) (prev >>> 32)) % maxSize;
            t = ((int) prev) % maxSize;
            next = (((long) h) << 32) | (LONG_BITS & t);
        } while (!UNSAFE.compareAndSwapLong(this, HEAD_OFFSET, prev, next));
    }

    private int getAndIncrementHead() {
        int h, t;
        long prev, next;
        do {
            prev = headAndTail;
            h = ((int) (prev >>> 32));
            t = (int) prev;
            if (h == t) {
                return INVALID_INDEX;
            }
            h += 1;
            next = (((long) h) << 32) | (LONG_BITS & t);
        } while (!UNSAFE.compareAndSwapLong(this, HEAD_OFFSET, prev, next));
        return (h - 1) % maxSize;
    }

    private int getAndIncrementTail() {
        int h, t;
        long prev, next;
        do {
            prev = headAndTail;
            h = (int) (prev >>> 32);
            t = (int) prev;
            if ((t > h) && (t - h) % maxSize == 0) {
                return INVALID_INDEX;
            }
            next = (((long) h) << 32) | (LONG_BITS & (t + 1));
        } while (!UNSAFE.compareAndSwapLong(this, HEAD_OFFSET, prev, next));
        return t % maxSize;
    }

    private static Unsafe initUnsafe() {
        Field f = null;
        try {
            f = Unsafe.class.getDeclaredField("theUnsafe");
        } catch (NoSuchFieldException e) {
            throw new NullPointerException("could not initialize UNSAFE");
        }
        f.setAccessible(true);
        try {
            return (Unsafe) f.get(null);
        } catch (IllegalAccessException e) {
            throw new NullPointerException("could not initialize UNSAFE");
        }
    }

    private static long getHeadOffset() {
        try {
            return UNSAFE.objectFieldOffset(
                    RingBuffer.class.getDeclaredField("headAndTail"));
        } catch (NoSuchFieldException e) {
            throw new NullPointerException("could not get field offset for headAndTail");
        }
    }

}
