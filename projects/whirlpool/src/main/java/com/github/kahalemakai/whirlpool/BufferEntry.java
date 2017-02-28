package com.github.kahalemakai.whirlpool;

import lombok.val;
import sun.misc.Unsafe;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

class BufferEntry<T> {

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

    private volatile long id;
    private volatile T value;
    private final ReentrantLock lock = new ReentrantLock(true);
    private final Condition signal = lock.newCondition();

    private BufferEntry(long id) {
        this.id = -id;
    }

    static <S> BufferEntry<S> ofId(long id) {
        return new BufferEntry<>(id);
    }

    public void set(long requestId, T element) throws InterruptedException {
        if (add(requestId, element)) {
//            signal.signalAll();
            return;
        }
        lock.lock();
        try {
            while (!add(requestId, element)) {
                signal.await();
            }
        } finally {
            lock.unlock();
            signal.signalAll();
        }
    }

    public T get(long requestId, int bufferSize) throws InterruptedException {
        T obj = take(requestId, bufferSize);
        if (obj != null) {
//            signal.signalAll();
            return obj;
        }
        lock.lock();
        try {
            while (true) {
                obj = take(requestId, bufferSize);
                if (obj != null) {
                    break;
                }
                signal.await();
            }
            return obj;
        } finally {
            lock.unlock();
            signal.signalAll();
        }
    }

    private T take(long requestId, int bufferSize) {
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

    private boolean add(long requestId, T element) {
        if (requestId != -id) {
            return false;
        }
        UNSAFE.putObjectVolatile(this, VALUE_OFFSET, element);
        UNSAFE.putLongVolatile(this, ID_OFFSET, requestId);
        return true;
    }

}
