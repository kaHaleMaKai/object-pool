package com.github.kahalemakai.whirlpool;

import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.val;

import java.lang.ref.WeakReference;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class PoolEntry<T> {

    private final T value;
    @Accessors(fluent = true) @Getter
    private final long expirationTimestamp;
    private final AtomicBoolean inUse;
    private final Whirlpool<T> pool;
    private final WeakReference<ScheduledFuture<?>> taskRef;

    private PoolEntry(T value, long expirationTime, Whirlpool<T> pool) {
        this.value = value;
        this.expirationTimestamp = System.currentTimeMillis() + expirationTime;
        this.inUse = new AtomicBoolean(false);
        this.pool = pool;
        this.taskRef = pool.scheduleEviction(this);
    }

    public boolean tryMarkAsUsed() {
        return inUse.compareAndSet(false, true);
    }

    public void cancelEviction() {
        val task = taskRef.get();
        if (task == null) {
            return;
        }
        task.cancel(false);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof PoolEntry)) {
            return false;
        }
        return value == ((PoolEntry) obj).rawValue();
    }

    public void evict() {
        try {
            pool.removeElement(this);
        } finally {
            pool.closeElement(value);
        }

    }

    public T value() {
        if (tryMarkAsUsed()) {
            return value;
        }
        return pool.createElement();
    }

    T rawValue() {
        return value;
    }

    static <S> PoolEntry<S> of(S element, long expirationTime, Whirlpool<S> pool) {
        return new PoolEntry<>(element, expirationTime, pool);
    }

}
