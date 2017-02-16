package com.github.kahalemakai.whirlpool.eviction;

import com.github.kahalemakai.whirlpool.ObjectPool;
import lombok.extern.log4j.Log4j;
import lombok.val;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

@Log4j
public class EvictionScheduler {

    private Timer timer;
    private final Object[] $lock = new Object[0];
    private final AtomicInteger usageCount = new AtomicInteger(0);
    private final Map<ObjectPool<?>, TimerTask> tracker;

    private EvictionScheduler() {
        val map = new WeakHashMap<ObjectPool<?>, TimerTask>();
        tracker = Collections.synchronizedMap(map);
    }

    public static EvictionScheduler init() {
        return new EvictionScheduler();
    }

    public boolean isActive() {
        return usageCount.get() == 0;
    }

    public void addPoolToSchedule(final ObjectPool<?> pool) {
        long expTime;
        synchronized ($lock) {
            if (tracker.containsKey(pool) && tracker.get(pool) != null) {
                return;
            }
            if (timer == null) {
                timer = new Timer("eviction-timer", true);
            }
            val task = EvictionTask.of(pool);
            // run slightly more often than necessary
            expTime = (long) (0.95 * pool.getExpirationTime());
            timer.scheduleAtFixedRate(task, expTime, expTime);
            tracker.put(pool, task);
            usageCount.getAndIncrement();
        }
        if (log.isDebugEnabled()) {
            val msg = String.format("scheduled %s for eviction every %d ms (with delay %d ms",
                    pool, expTime, expTime);
            log.debug(msg);
        }
    }

    public void removePoolFromSchedule(final ObjectPool<?> pool) {
        synchronized ($lock) {
            if (usageCount.get() == 0) {
                return;
            }
            val task = tracker.remove(pool);
            if (task == null) {
                return;
            }
            task.cancel();
            if (usageCount.decrementAndGet() == 0) {
                timer.cancel();
                timer = null;
            }
        }
        if (log.isDebugEnabled()) {
            val msg = String.format("removed %s from background eviction", pool);
            log.debug(msg);
        }
    }
}
