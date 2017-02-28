package com.github.kahalemakai.whirlpool;

import lombok.Getter;
import lombok.val;

import java.util.concurrent.atomic.AtomicLong;

public class RingBuffer<T> {

    private final AtomicLong head, tail;
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
        this.head = new AtomicLong(1);
        this.tail = new AtomicLong(1);
    }

    public void offer(T element) {
        if (element == null) {
            throw new IllegalArgumentException("RingBuffer does not accept null elements");
        }
        val requestId = tail.getAndIncrement();
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
        val requestId = head.getAndIncrement();
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
        return (int) (tail.get() - head.get());
    }

}
