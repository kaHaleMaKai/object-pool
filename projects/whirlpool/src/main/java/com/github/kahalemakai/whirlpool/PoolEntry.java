package com.github.kahalemakai.whirlpool;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.experimental.Accessors;
import sun.misc.Unsafe;

import java.lang.reflect.Field;

class PoolEntry<T> {

    private static final Unsafe UNSAFE;
    static {
        UNSAFE = initUnsafe();
    }

    private static final long IN_USE_OFFSET;
    static {
        IN_USE_OFFSET = getInUseOffset();
    }

    @Accessors(fluent = true) @Getter
    private final long expirationTimestamp;
    @Accessors(fluent = true)
    @Getter
    private final T value;

    @Getter(AccessLevel.PROTECTED)
    private final Whirlpool<T> pool;

    private int inUse;

    private PoolEntry(T value, long expirationTime, Whirlpool<T> pool) {
        this.value = value;
        this.pool = pool;
        this.expirationTimestamp = System.currentTimeMillis() + expirationTime;
    }

    public boolean tryMarkAsUsed() {
        return UNSAFE.compareAndSwapInt(this, IN_USE_OFFSET, 0, 1);
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
        if (!(obj instanceof PoolEntry)) {
            return false;
        }
        return value == ((PoolEntry<?>) obj).value();
    }

    static <S> PoolEntry<S> of(S element, long expirationTime, Whirlpool<S> pool) {
        return new PoolEntry<>(element, expirationTime, pool);
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

    private static long getInUseOffset() {
        try {
            return UNSAFE.objectFieldOffset(
                    PoolEntry.class.getDeclaredField("inUse"));
        } catch (NoSuchFieldException e) {
            throw new NullPointerException("could not blockAndGet field offset for inUse");
        }
    }

}
