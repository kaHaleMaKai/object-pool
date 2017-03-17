package com.github.kahalemakai.whirlpool;

import lombok.Getter;
import lombok.val;
import sun.misc.Contended;
import sun.misc.Unsafe;

import java.util.function.Consumer;

public class RingBuffer<T> {

    private static final Unsafe UNSAFE;
    static {
        UNSAFE = UnsafeManager.getUnsafe();
    }

    private static final long HEAD_OFFSET, TAIL_OFFSET;
    static {
        HEAD_OFFSET = UnsafeManager.getOffset(RingBuffer.class, "head");
        TAIL_OFFSET = UnsafeManager.getOffset(RingBuffer.class, "tail");
    }

    @Contended
    private volatile long head;
    @Contended
    private volatile long tail;

    private final BufferEntry<T>[] buffer;
    @Getter
    private final int capacity;

    RingBuffer(int capacity) {
        this.capacity = capacity;
        @SuppressWarnings("unchecked")
        val array = (BufferEntry<T>[]) new BufferEntry<?>[capacity];
        for (int i = 0; i < array.length; i++) {
            array[i] = BufferEntry.ofId(i + 1);
        }
        this.buffer = array;
        this.head = 1;
        this.tail = 1;
    }

    public boolean offer(T element) {
        throwIfNull(element);
    }

    public T poll() {
        // fail fast
        if (size() == 0) {
            return null;
        }
        val requestId = UNSAFE.getAndAddLong(this, HEAD_OFFSET, 1);
        val idx = (int) ((requestId - 1) % capacity);
        try {
            val entry = this.buffer[idx];
            val value = entry.get(requestId, capacity);
            if (value != null) {
                return value;
            }

        } catch (InterruptedException e) {
            // TODO some cleanup should be necessary in
            // TODO the buffer entry, e.g. signaling
            Thread.currentThread().interrupt();
        }
        return null;

    }

    public void put(T element) {
        throwIfNull(element);
        val requestId = UNSAFE.getAndAddLong(this, TAIL_OFFSET, 1);
        val idx = (int) ((requestId - 1) % capacity);
        try {
            this.buffer[idx].blockAndSet(requestId, element);
        } catch (InterruptedException e) {
            // TODO some cleanup should be necessary in
            // TODO the buffer entry, e.g. signaling
            Thread.currentThread().interrupt();
        }
    }

    public T take() {
        val requestId = UNSAFE.getAndAddLong(this, HEAD_OFFSET, 1);
        val idx = (int) ((requestId - 1) % capacity);
        try {
            return this.buffer[idx].blockAndGet(requestId, capacity);
        } catch (InterruptedException e) {
            // TODO some cleanup should be necessary in
            // TODO the buffer entry, e.g. signaling
            Thread.currentThread().interrupt();
        }
        return null;
    }

    public int size() {
        return (int) (tail - head);
    }

    public T peek() {
        val idx = (int) ((head - 1) % capacity);
        return this.buffer[idx].getValue();
    }

    public boolean triggerExpiration(long deadline, Consumer<T> evictor) {
        if (size() == 0) {
            return false;
        }
        boolean anyEvicted = false;
        while (true) {
            val head = expireHead(deadline);
            if (head == null) {
                break;
            }
            anyEvicted = true;
            evictor.accept(head);
        }
        return anyEvicted;
    }

    private T expireHead(long deadline) {
        val requestId = head;
        val idx = (int) ((requestId - 1) % capacity);
        val entry = this.buffer[idx];
        if (!entry.casId(requestId, 0)) {
            return null;
        }
        if (!(entry.getTimestamp() < deadline)
                || !UNSAFE.compareAndSwapLong(this, HEAD_OFFSET, requestId, HEAD_OFFSET + 1)) {
            entry.casId(0, requestId);
            return null;
        }
        try {
            return entry.blockAndGet(requestId, capacity);
        } catch (InterruptedException e) {
            Thread.interrupted();
            return null;
        }
    }

    private static void throwIfNull(Object obj) {
        if (obj == null) {
            throw new IllegalArgumentException("RingBuffer does not accept null elements");
        }
    }

}
