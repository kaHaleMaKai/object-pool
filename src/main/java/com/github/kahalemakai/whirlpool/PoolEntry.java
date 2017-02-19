package com.github.kahalemakai.whirlpool;

public interface PoolEntry<T> {
    boolean tryMarkAsUsed();

    void evict();

    T value();

    T rawValue();

    long expirationTimestamp();

    void cancelEviction();

    static <S> PoolEntry<S> of(S element, long expirationTime, Whirlpool<S> pool) {
        if (expirationTime == Whirlpool.INFINITE_EXPIRATION_TIME) {
            return new NonExpiringPoolEntry<>(element, pool);
        }
        return new ExpiringPoolEntry<>(element, expirationTime, pool);
    }

}
