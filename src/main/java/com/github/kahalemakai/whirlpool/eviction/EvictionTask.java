package com.github.kahalemakai.whirlpool.eviction;

import com.github.kahalemakai.whirlpool.ObjectPool;
import lombok.extern.log4j.Log4j;

import java.lang.ref.WeakReference;
import java.util.Objects;
import java.util.TimerTask;

@Log4j
class EvictionTask extends TimerTask {
    private static final long EVICTION_TIMEOUT = 100;

    private final WeakReference<? extends ObjectPool<?>> poolRef;

    ObjectPool<?> getPool() {
        return poolRef.get();
    }

    private EvictionTask(final ObjectPool<?> pool) {
        this.poolRef = new WeakReference<ObjectPool<?>>(pool);
    }

    @Override
    public void run() {
        ObjectPool<?> pool = poolRef.get();
        if (pool == null) {
            this.cancel();
            return;
        }
        if (pool.availableElementsEstimate() > 0) {
            try {
                pool.evictAll(EVICTION_TIMEOUT);
            } catch (InterruptedException e) {
                log.warn("background eviction task timed out");
                Thread.interrupted();
            }
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * {@code EvictionTask}s {@code hashCode} method
     * method delegates to the weakly-referenced
     * pool object.
     */
    @Override
    public int hashCode() {
        return Objects.hashCode(poolRef.get());
    }

    /**
     * {@inheritDoc}
     * <p>
     * {@code EvictionTask}s are compared by object identify
     * of the weakly-referenced pool object.
     *
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || !(obj instanceof EvictionTask)) {
            return false;
        }
        return poolRef.get() == ((EvictionTask) obj).getPool();
    }


    static EvictionTask of(final ObjectPool<?> pool) {
        return new EvictionTask(pool);
    }

}
