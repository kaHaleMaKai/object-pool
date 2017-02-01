package com.github.kahalemakai.whirlpool;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j;
import lombok.val;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Basic implementation of the {@link Poolable} interface.
 * <p>
 * Besides implementing the interface, periodic eviction of
 * expired elements in the object pool may be set up
 * by calling {@link #scheduleForEviction()}.
 * @param <T>
 *     type of objects in the pool
 */
@Log4j
public class Whirlpool<T> implements Poolable<T> {

    private static final long SECOND = 1000;
    private static final long MINUTE = 60 * SECOND;

    public static final long DEFAULT_EXPIRATION_TIME = 30 * SECOND;
    private static final long EVICTION_INTERVAL = MINUTE;
    private static final long EVICTION_DELAY = 5 * MINUTE;
    private static final long EVICTION_TIMEOUT = 100;

    private static final Set<Whirlpool<?>> poolTracker;
    static {
        val map = new WeakHashMap<Whirlpool<?>, Boolean>();
        val syncedMap = Collections.synchronizedMap(map);
        poolTracker = Collections.newSetFromMap(syncedMap);
    }

    private static final Timer evictionTimer;
    static {
        evictionTimer = new Timer("eviction-timer");
        val task = EvictionTask.init();
        evictionTimer.scheduleAtFixedRate(task, EVICTION_DELAY, EVICTION_INTERVAL);
    }

    private static final Supplier<?> DEFAULT_CREATE_FN;
    static {
        DEFAULT_CREATE_FN = () -> {
            throw new UnsupportedOperationException("missing Supplier to create elements");
        };
    }

    private static final Consumer<?> DEFAULT_CLOSE_FN;
    static {
        DEFAULT_CLOSE_FN = (t) -> { };
    }

    private volatile int availableElements;
    private volatile int elementsCreated;

    /**
     * @return {@inheritDoc}
     */
    @Getter
    private final long expirationTime;

    private final Set<T> inUse;
    private final Map<T, Long> expiring;
    private final LinkedList<T> orderedExpiringObjects;

    private final Lock $lock;

    private final Supplier<T> createFn;
    private final Consumer<T> closeFn;

    public Whirlpool(final long expirationTime,
                     final @NonNull Supplier<T> createFn,
                     final @NonNull Consumer<T> closeFn) {
        this.expirationTime = expirationTime;
        this.inUse = new HashSet<>();
        this.expiring = new HashMap<>();
        this.$lock = new ReentrantLock();
        this.orderedExpiringObjects = new LinkedList<>();
        this.createFn = createFn;
        this.closeFn = closeFn;
    }

    @SuppressWarnings("unchecked")
    public Whirlpool(final long expirationTime,
                     final Supplier<T> createFn) {
        this(expirationTime, createFn, (Consumer<T>) DEFAULT_CLOSE_FN);
    }

    @SuppressWarnings("unchecked")
    public Whirlpool(final long expirationTime) {
        this(expirationTime, (Supplier<T>) DEFAULT_CREATE_FN);
    }

    public Whirlpool() {
        this(DEFAULT_EXPIRATION_TIME);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean validate(T element) {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close(T element) {
        this.closeFn.accept(element);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T createElement() {
        return createFn.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T borrow() {
        $lock.lock();
        try {
            return borrowHelper();
        } finally {
            $lock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T borrow(long millis) throws InterruptedException {
        $lock.tryLock(millis, TimeUnit.MILLISECONDS);
        try {
            return borrowHelper();
        } finally {
            $lock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void unhand(T element) {
        $lock.lock();
        try {
            unhandHelper(element);
        } finally {
            $lock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void unhand(T element, long millis) throws InterruptedException {
        $lock.tryLock(millis, TimeUnit.MILLISECONDS);
        try {
            unhandHelper(element);
        } finally {
            $lock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int availableElements() {
        $lock.lock();
        try {
            return availableElements;
        } finally {
            $lock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int availableElementsEstimate() {
        return availableElements;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int elementsCreated() {
        $lock.lock();
        try {
            return elementsCreated;
        } finally {
            $lock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int elementsCreatedEstimate() {
        return elementsCreated;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void evictAll() {
        if (!evictionPossible()) {
            return;
        }
        $lock.lock();
        try {
            evictHelper();
        } finally {
            $lock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void evictAll(long millis) throws InterruptedException {
        if (!evictionPossible()) {
            return;
        }
        $lock.tryLock(millis, TimeUnit.MILLISECONDS);
        try {
            evictHelper();
        } finally {
            $lock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void evict(T element) {
        $lock.lock();
        try {
            if (availableElements == 0) {
                return;
            }
            evictHelper(element);
        } finally {
            $lock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void evict(T element, long millis) throws InterruptedException {
        if (availableElementsEstimate() == 0) {
            return;
        }
        $lock.tryLock(millis, TimeUnit.MILLISECONDS);
        try {
            evictHelper(element);
        } finally {
            $lock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeNow(T element) {
        if (availableElementsEstimate() == 0) {
            return;
        }
        $lock.lock();
        try {
            removeNowHelper(element);
        } finally {
            $lock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeNow(T element, long millis) throws InterruptedException {
        if (availableElementsEstimate() == 0) {
            return;
        }
        $lock.tryLock(millis, TimeUnit.MILLISECONDS);
        try {
            removeNowHelper(element);
        } finally {
            $lock.unlock();
        }
    }

    /**
     * Run eviction on this object pool periodically in the background.
     * <p>
     * A background thread is created and associated with
     * the {@link Whirlpool} class object.
     */
    public void scheduleForEviction() {
        Whirlpool.poolTracker.add(this);
    }

    /**
     * Don't run eviction on this object pool peridically in the background.
     */
    public void removeFromEvictionSchedule() {
        Whirlpool.poolTracker.remove(this);
    }

    private void evictHelper() {
        int numEvicted = 0;
        val shiftedCurrentTimestamp = System.currentTimeMillis() - expirationTime;
        while (orderedExpiringObjects.size() > 0) {
            val head = orderedExpiringObjects.getFirst();
            val timestamp = expiring.get(head);
            if (timestamp <= shiftedCurrentTimestamp) {
                expiring.remove(head);
                orderedExpiringObjects.removeFirst();
                this.close(head);
                numEvicted++;
            }
            else {
                break;
            }
        }
        availableElements -= numEvicted;
        elementsCreated -= numEvicted;
    }

    private void evictHelper(T element) {
        evictOrRemoveNow(element, false);
    }

    private void removeNowHelper(T element) {
        evictOrRemoveNow(element, true);
    }

    private void evictOrRemoveNow(T element, boolean instantly) {
        val timestamp = expiring.get(element);
        if (timestamp == null) {
            return;
        }
        if (!instantly) {
            val shiftedCurrentTimestamp = System.currentTimeMillis() - expirationTime;
            if (timestamp > shiftedCurrentTimestamp) {
                return;
            }
        }
        for (int i = 0; i < orderedExpiringObjects.size(); ++i) {
            val exp = orderedExpiringObjects.get(i);
            if (exp.equals(element)) {
                orderedExpiringObjects.remove(i);
                expiring.remove(element);
                availableElements--;
                elementsCreated--;
                this.close(element);
                break;
            }
        }
    }

    private boolean evictionPossible() {
        if (availableElementsEstimate() == 0) {
            return false;
        }
        try {
            val first = orderedExpiringObjects.getFirst();
            val timestamp = expiring.get(first);
            return timestamp <= System.currentTimeMillis() - expirationTime;
        } catch (NoSuchElementException e) {
            return false;
        }
    }

    private T borrowHelper() {
        T element;
        if (availableElements > 0) {
            element = orderedExpiringObjects.pollFirst();
            expiring.remove(element);
            if (!validate(element)) {
                val msg = String.format(
                        "cannot validate borrowed element %s. creating a new one instead",
                        element);
                log.info(msg);
            }
            
            availableElements--;
        } else {
            element = createElement();
            elementsCreated++;
        }
        inUse.add(element);
        return element;
    }

    private void unhandHelper(T element) {
        inUse.remove(element);
        expiring.put(element, System.currentTimeMillis());
        orderedExpiringObjects.addLast(element);
        availableElements++;
    }

    @RequiredArgsConstructor(staticName = "init")
    private static class EvictionTask extends TimerTask {
        final Queue<Whirlpool<?>> queue = new LinkedBlockingQueue<>();

        @Override
        public void run() {
            for (val pool : poolTracker) {
                if (pool.availableElements() > 0) {
                    try {
                        pool.evictAll(EVICTION_TIMEOUT);
                    } catch (InterruptedException e) {
                        Thread.interrupted();
                        queue.add(pool);
                    }
                }
            }
            while (queue.size() > 0) {
                val pool = queue.poll();
                if (pool.availableElements() > 0) {
                    try {
                        pool.evictAll(EVICTION_TIMEOUT);
                    } catch (InterruptedException e) {
                        Thread.interrupted();
                        log.debug("could not evictAll object from pool");
                    }
                }
            }
        }
    }

}
