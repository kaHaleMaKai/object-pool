package com.github.kahalemakai.whirlpool;

import lombok.*;
import lombok.extern.log4j.Log4j;

import java.lang.ref.WeakReference;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Predicate;
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

    private static final Map<Poolable<?>, TimerTask> poolTracker;
    static {
        val map = new WeakHashMap<Poolable<?>, TimerTask>();
        poolTracker = Collections.synchronizedMap(map);
    }

    private static final Timer evictionTimer;
    static {
        evictionTimer = new Timer("eviction-timer");
    }

    private static final Supplier<?> DEFAULT_CREATE_FN;
    static {
        DEFAULT_CREATE_FN = () -> {
            throw new UnsupportedOperationException("missing Supplier to create elements");
        };
    }

    private static final Predicate<?> DEFAULT_VALIDATION_FN;
    static {
        DEFAULT_VALIDATION_FN = (t) -> true;
    }

    private static final Consumer<?> NO_OP_CONSUMER;
    static {
        NO_OP_CONSUMER = (t) -> { };
    }

    private volatile int availableElements;
    private volatile int totalSize;

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
    private final Predicate<T> validationFn;
    private final Consumer<T> closeFn;
    private final Consumer<T> prepareFn;
    private final Consumer<T> resetFn;

    private TimerTask evictionTask = null;

    @Builder
    public Whirlpool(final long expirationTime,
                     final Supplier<T> onCreate,
                     final Predicate<T> onValidation,
                     final Consumer<T> onClose,
                     final Consumer<T> onPrepare,
                     final Consumer<T> onReset) {
        this.expirationTime = expirationTime;
        this.inUse = new HashSet<>();
        this.expiring = new HashMap<>();
        this.$lock = new ReentrantLock();
        this.orderedExpiringObjects = new LinkedList<>();
        if (onCreate == null) {
            @SuppressWarnings("unchecked")
            final Supplier<T> createFn = (Supplier<T>) DEFAULT_CREATE_FN;
            this.createFn = createFn;
        }
        else {
            this.createFn = onCreate;
        }
        if (onClose == null) {
            @SuppressWarnings("unchecked")
            final Consumer<T> closeFn = (Consumer<T>) NO_OP_CONSUMER;
            this.closeFn = closeFn;
        }
        else {
            this.closeFn = onClose;
        }
        if (onValidation == null) {
            @SuppressWarnings("unchecked")
            final Predicate<T> validationFn = (Predicate<T>) DEFAULT_VALIDATION_FN;
            this.validationFn = validationFn;
        }
        else {
            this.validationFn = onValidation;
        }
        if (onReset == null) {
            @SuppressWarnings("unchecked")
            final Consumer<T> resetFn = (Consumer<T>) NO_OP_CONSUMER;
            this.resetFn = resetFn;
        }
        else {
            this.resetFn = onReset;
        }
        if (onPrepare == null) {
            @SuppressWarnings("unchecked")
            final Consumer<T> prepareFn = (Consumer<T>) NO_OP_CONSUMER;
            this.prepareFn = prepareFn;
        }
        else {
            this.prepareFn = onPrepare;
        }
    }

    @SuppressWarnings("unchecked")
    public Whirlpool(final long expirationTime,
                     final Supplier<T> createFn,
                     final Predicate<T> validationFn) {
        this(expirationTime,
                createFn,
                validationFn,
                (Consumer<T>) NO_OP_CONSUMER,
                (Consumer<T>) NO_OP_CONSUMER,
                (Consumer<T>) NO_OP_CONSUMER);
    }

    @SuppressWarnings("unchecked")
    public Whirlpool(final long expirationTime,
                     final Supplier<T> createFn) {
        this(expirationTime, createFn, (Predicate<T>) DEFAULT_VALIDATION_FN);
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
        return validationFn.test(element);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void closeElement(T element) {
        if (log.isDebugEnabled()) {
            val msg = "closing pooled object " + element;
            log.debug(msg);
        }
        this.closeFn.accept(element);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T createElement() {
        val element = createFn.get();
        if (log.isDebugEnabled()) {
            val msg = "creating new element " + element;
            log.debug(msg);
        }
        return element;
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
        if ($lock.tryLock(millis, TimeUnit.MILLISECONDS)) {
            try {
                return borrowHelper();
            } finally {
                $lock.unlock();
            }
        }
        val msg = String.format("timed out while borrowing on thread %s",
                Thread.currentThread().getName());
        throw new InterruptedException(msg);
    }

    public AutoClosing<T> borrowKindly() {
        return AutoClosing.of(borrow(), this);
    }

    public AutoClosing<T> borrowKindly(long millis) throws InterruptedException {
        return AutoClosing.of(borrow(millis), this);
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
        if ($lock.tryLock(millis, TimeUnit.MILLISECONDS)) {
            try {
                unhandHelper(element);
                return;
            } finally {
                $lock.unlock();
            }
        }
        val msg = String.format("timed out while unhanding object on thread %s",
                Thread.currentThread().getName());
        throw new InterruptedException(msg);
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
    public int totalSize() {
        $lock.lock();
        try {
            return totalSize;
        } finally {
            $lock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int totalSizeEstimate() {
        return totalSize;
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
        if ($lock.tryLock(millis, TimeUnit.MILLISECONDS)) {
            try {
                evictHelper();
                return;
            } finally {
                $lock.unlock();
            }
        }
        val msg = String.format("timed out while evicting all expired objects on thread %s",
                Thread.currentThread().getName());
        throw new InterruptedException(msg);
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
        if ($lock.tryLock(millis, TimeUnit.MILLISECONDS)) {
            try {
                evictHelper(element);
                return;
            } finally {
                $lock.unlock();
            }
        }
        val msg = String.format("timed out while evicting an object on thread %s",
                Thread.currentThread().getName());
        throw new InterruptedException(msg);
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
        if ($lock.tryLock(millis, TimeUnit.MILLISECONDS)) {
            try {
                removeNowHelper(element);
                return;
            } finally {
                $lock.unlock();
            }
        }
        val msg = String.format("timed out while removing an object on thread %s",
                Thread.currentThread().getName());
        throw new InterruptedException(msg);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void prepareElement(T element) {
        this.prepareFn.accept(element);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void resetElement(T element) {
        this.resetFn.accept(element);
    }


    /**
     * Run eviction on this object pool periodically in the background.
     * <p>
     * All eviction tasks are scheduled by a single timer, so adding
     * a new pool should not account for a performance penalty.
     * <p>
     * This method will only add a task on its first call, or after
     * calling {@link #removeFromEvictionSchedule()}.
     */
    public void scheduleForEviction() {
        Whirlpool.addPoolToSchedule(this);
    }

    /**
     * Remove an object pool from eviction in background, if
     * it has been registered before.
     */
    public void removeFromEvictionSchedule() {
        Whirlpool.removePoolFromSchedule(this);
    }

    /* ******************************************************
     *                    public method                    *
     * *****************************************************/

    private void evictHelper() {
        int numEvicted = 0;
        val shiftedCurrentTimestamp = System.currentTimeMillis() - expirationTime;
        while (orderedExpiringObjects.size() > 0) {
            val head = orderedExpiringObjects.getFirst();
            val timestamp = expiring.get(head);
            if (timestamp <= shiftedCurrentTimestamp) {
                expiring.remove(head);
                orderedExpiringObjects.removeFirst();
                this.closeElement(head);
                numEvicted++;
            }
            else {
                break;
            }
        }
        availableElements -= numEvicted;
        totalSize -= numEvicted;
        if (log.isDebugEnabled()) {
            val msg = String.format("evicted %d element(s), idle element(s): %d, total number: %d",
                   numEvicted, availableElements, totalSize);
            log.debug(msg);
        }
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
                totalSize--;
                this.closeElement(element);
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
        T element = null;
        if (availableElements > 0) {
            element = orderedExpiringObjects.pollFirst();
            expiring.remove(element);
            if (!validate(element)) {
                if (log.isDebugEnabled()) {
                    val msg = String.format(
                            "cannot validate borrowed element %s. creating a new one instead",
                            element);
                    log.debug(msg);
                }
                this.closeElement(element);
                element = null;
            }
            availableElements--;
        }
        if (element == null) {
            element = createElement();
            totalSize++;
        }
        else {
            this.prepareElement(element);
        }
        inUse.add(element);
        return element;
    }

    private void unhandHelper(T element) {
        inUse.remove(element);
        this.resetElement(element);
        expiring.put(element, System.currentTimeMillis());
        orderedExpiringObjects.addLast(element);
        availableElements++;
    }

    /* ******************************************************
     *                     static method                    *
     * *****************************************************/

    private static void addPoolToSchedule(final Poolable<?> pool) {
        long expTime;
        synchronized (poolTracker) {
            if (poolTracker.containsKey(pool) && poolTracker.get(pool) != null) {
                return;
            }
            val task = EvictionTask.of(pool);
            // run slightly more often than necessary
            expTime = (long) (0.95 * pool.getExpirationTime());
            evictionTimer.scheduleAtFixedRate(task, expTime, expTime);
            poolTracker.put(pool, task);
        }
        if (log.isDebugEnabled()) {
            val msg = String.format("scheduled %s for eviction every %d ms (with delay %d ms",
                    pool, expTime, expTime);
            log.debug(msg);
        }
    }

    private static void removePoolFromSchedule(final Poolable<?> pool) {
        synchronized (poolTracker) {
            val task = poolTracker.remove(pool);
            if (task == null) {
                return;
            }
            task.cancel();
        }
        if (log.isDebugEnabled()) {
            val msg = String.format("removed %s from background eviction", pool);
            log.debug(msg);
        }
    }

    private static class EvictionTask extends TimerTask {

        private final WeakReference<? extends Poolable<?>> poolRef;

        Poolable<?> getPool() {
            return poolRef.get();
        }

        private EvictionTask(final Poolable<?> pool) {
            this.poolRef = new WeakReference<Poolable<?>>(pool);
        }

        @Override
        public void run() {
            Poolable<?> pool = poolRef.get();
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


        private static EvictionTask of(final Poolable<?> pool) {
            return new EvictionTask(pool);
        }

    }

}
