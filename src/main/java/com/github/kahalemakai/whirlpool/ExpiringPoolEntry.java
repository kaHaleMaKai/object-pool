package com.github.kahalemakai.whirlpool;

import lombok.Getter;
import lombok.experimental.Accessors;
import lombok.val;

import java.lang.ref.WeakReference;
import java.util.concurrent.ScheduledFuture;

public class ExpiringPoolEntry<T> extends NonExpiringPoolEntry<T> {

    @Accessors(fluent = true) @Getter
    private final long expirationTimestamp;
    private final WeakReference<ScheduledFuture<?>> taskRef;

    ExpiringPoolEntry(T value, long expirationTime, Whirlpool<T> pool) {
        super(value, pool);
        this.expirationTimestamp = System.currentTimeMillis() + expirationTime;
        this.taskRef = pool.scheduleEviction(this);
    }

    public void cancelEviction() {
        val task = taskRef.get();
        if (task == null) {
            return;
        }
        task.cancel(false);
    }

}
