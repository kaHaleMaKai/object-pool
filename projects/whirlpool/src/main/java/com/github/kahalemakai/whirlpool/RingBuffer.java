package com.github.kahalemakai.whirlpool;

import lombok.Getter;
import lombok.val;
import sun.misc.Contended;
import sun.misc.Unsafe;

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
    private final int maxSize;

    RingBuffer(Class<? extends T> type, int maxSize) {
        this.maxSize = maxSize;
        @SuppressWarnings("unchecked")
        val array = (BufferEntry<T>[]) new BufferEntry<?>[maxSize];
        for (int i = 0; i < array.length; i++) {
            array[i] = BufferEntry.ofId(i + 1);
        }
        this.buffer = array;
        this.head = 1;
        this.tail = 1;
    }

    public void offer(T element) {
        if (element == null) {
            throw new IllegalArgumentException("RingBuffer does not accept null elements");
        }
        val requestId = UNSAFE.getAndAddLong(this, TAIL_OFFSET, 1);
        val idx = (int) ((requestId - 1) % maxSize);
        try {
            this.buffer[idx].set(requestId, element);
        } catch (InterruptedException e) {
            // TODO some cleanup should be necessary in
            // TODO the buffer entry, e.g. signaling
            Thread.currentThread().interrupt();
        }
    }

    public T poll() {
        val requestId = UNSAFE.getAndAddLong(this, HEAD_OFFSET, 1);
        val idx = (int) ((requestId - 1) % maxSize);
        try {
            return this.buffer[idx].get(requestId, maxSize);
        } catch (InterruptedException e) {
            // TODO some cleanup should be necessary in
            // TODO the buffer entry, e.g. signaling
            Thread.currentThread().interrupt();
        }
        return null;
    }

    public int availableElements() {
        return (int) (tail - head);
    }

}
