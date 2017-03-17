package com.github.kahalemakai.whirlpool;

import lombok.Getter;
import lombok.val;
import sun.misc.Contended;
import sun.misc.Unsafe;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

class BufferEntry<T> {

    private static final long WAITING_INTERVAL = 50;
    private static final long INVALID_TIMESTAMP = Long.MAX_VALUE;

    private static final Unsafe UNSAFE;
    static {
        UNSAFE = UnsafeManager.getUnsafe();
    }

    private static final long ID_OFFSET;
    static {
        ID_OFFSET = UnsafeManager.getOffset(BufferEntry.class, "id");
    }

    private static final long VALUE_OFFSET;
    static {
        VALUE_OFFSET = UnsafeManager.getOffset(BufferEntry.class, "value");
    }

    @Contended
    private volatile long id;
    @Getter
    @Contended
    private volatile T value;
    @Getter
    @Contended
    private volatile long timestamp = INVALID_TIMESTAMP;

    private final ReentrantLock lock = new ReentrantLock(true);
    private final Condition signal = lock.newCondition();

    private BufferEntry(long id) {
        this.id = -id;
    }

    static <S> BufferEntry<S> ofId(long id) {
        return new BufferEntry<>(id);
    }

    public void blockAndSet(long requestId, T element) throws InterruptedException {
        if (set(requestId, element)) {
            return;
        }
        lock.lock();
        try {
            while (!set(requestId, element)) {
                signal.awaitNanos(WAITING_INTERVAL);
            }
        } finally {
            signal.signalAll();
            lock.unlock();
        }
    }

    public T blockAndGet(long requestId, int bufferSize) throws InterruptedException {
        T obj = get(requestId, bufferSize);
        if (obj != null) {
            return obj;
        }
        lock.lock();
        try {
            while (true) {
                obj = get(requestId, bufferSize);
                if (obj != null) {
                    break;
                }
                signal.awaitNanos(WAITING_INTERVAL);
            }
            return obj;
        } finally {
            signal.signalAll();
            lock.unlock();
        }
    }

    public T get(long requestId, int bufferSize) {
        if (requestId != id) {
            return null;
        }
        @SuppressWarnings("unchecked")
        final T obj = value;
        UNSAFE.putObjectVolatile(this, VALUE_OFFSET, null);
        val nextId = -(id + bufferSize);
        UNSAFE.putLongVolatile(this, ID_OFFSET, nextId);
        return obj;
    }

    public boolean set(long requestId, T element) {
        if (requestId != -id) {
            return false;
        }
        UNSAFE.putObjectVolatile(this, VALUE_OFFSET, element);
        UNSAFE.putLongVolatile(this, ID_OFFSET, requestId);
        this.timestamp = System.currentTimeMillis();
        return true;
    }

    boolean casId(long from, long to) {
        return UNSAFE.compareAndSwapLong(this, ID_OFFSET, from, to);
    }

}
