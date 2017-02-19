package com.github.kahalemakai.whirlpool;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.experimental.Accessors;

import java.util.concurrent.atomic.AtomicBoolean;

class NonExpiringPoolEntry<T> implements PoolEntry<T> {

    private final T value;
    @Accessors(fluent = true) @Getter
    private final AtomicBoolean inUse;
    @Getter(AccessLevel.PROTECTED)
    private final Whirlpool<T> pool;

    NonExpiringPoolEntry(T value, Whirlpool<T> pool) {
        this.value = value;
        this.inUse = new AtomicBoolean(false);
        this.pool = pool;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean tryMarkAsUsed() {
        return inUse.compareAndSet(false, true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return value.hashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof ExpiringPoolEntry)) {
            return false;
        }
        return value == ((ExpiringPoolEntry) obj).rawValue();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void evict() {
        try {
            getPool().removeElement(this);
        } finally {
            pool.closeElement(value);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T value() {
        if (tryMarkAsUsed()) {
            return value;
        }
        return pool.createElement();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T rawValue() {
        return value;
    }

    /**
     * {@inheritDoc}
     * It will not expire, ever.
     */
    @Override
    public long expirationTimestamp() {
        return Long.MAX_VALUE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void cancelEviction() { }

}
