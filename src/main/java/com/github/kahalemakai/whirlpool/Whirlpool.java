package com.github.kahalemakai.whirlpool;

import com.github.kahalemakai.whirlpool.eviction.AutoClosing;
import com.github.kahalemakai.whirlpool.eviction.EvictionScheduler;
import lombok.*;
import lombok.extern.log4j.Log4j;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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

    public static final long DEFAULT_EXPIRATION_TIME = 30 * SECOND;

    private static final EvictionScheduler EVICTION_SCHEDULER;
    static {
        EVICTION_SCHEDULER = EvictionScheduler.init();
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

    private final AtomicInteger availableElements = new AtomicInteger(0);
    private final AtomicInteger totalSize = new AtomicInteger(0);

    /**
     * @return {@inheritDoc}
     */
    @Getter
    private final long expirationTime;

    // TODO use weak references (and weak key maps etc. for pool objects)
    // setup referencequeues and close objects regularly
    // could be scheduled along with eviction
    private final Set<T> inUse;
    private final Map<T, Long> expiring;
    private final LinkedList<T> orderedExpiringObjects;

    private final Lock $lock;

    private final Supplier<T> createFn;
    private final Predicate<T> validationFn;
    private final Consumer<T> closeFn;
    private final Consumer<T> prepareFn;
    private final Consumer<T> resetFn;

    private final AtomicBoolean closed = new AtomicBoolean(false);

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
        throwIfClosed();
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
        throwIfClosed();
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
        throwIfClosed();
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
        throwIfClosed();
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
            return availableElements.get();
        } finally {
            $lock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int availableElementsEstimate() {
        return availableElements.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int totalSize() {
        $lock.lock();
        try {
            return totalSize.get();
        } finally {
            $lock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int totalSizeEstimate() {
        return totalSize.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void evictAll() {
        throwIfClosed();
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
    public void close() throws PoolException {
        if (closed.get()) {
            return;
        }
        $lock.lock();
        try {
            removeHelper();
        } catch (Exception e) {
            throw new PoolException(e);
        } finally {
            closed.set(true);
            $lock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void evictAll(long millis) throws InterruptedException {
        throwIfClosed();
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
     * Evict a specific object from the pool, if it is available
     * and has expired.
     * <p>
     * This method should block. An unchecked exception may be thrown
     * if the element is not present, but it may be silently ignored
     * as well.
     * @param element
     *     the object to evict from the pool
     * @throws PoolException
     *     if a {@link #closeElement(Object)} method needs to be
     *     called, and fails
     */
    public void evict(T element) {
        throwIfClosed();
        $lock.lock();
        try {
            if (availableElements.get() == 0) {
                return;
            }
            evictHelper(element);
        } finally {
            $lock.unlock();
        }
    }

    /**
     * Evict a specific object from the pool, if it is available
     * and has expired.
     * <p>
     * This method should timeout after {@code millis} milliseconds.
     * An unchecked exception may be thrown
     * if the element is not present, but it may be silently ignored
     * as well.
     * @param element
     *     the object to evict from the pool
     * @param millis
     *     timeout in milliseconds
     * @throws InterruptedException
     *     if timed out
     * @throws PoolException
     *     if a {@link #closeElement(Object)} method needs to be
     *     called, and fails
     */
    public void evict(T element, long millis) throws InterruptedException {
        throwIfClosed();
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
     * Remove an object now from the pool, if it is available.
     * <p>
     * This method should block. An unchecked exception may be thrown
     * if the element is not present, but it may be silently ignored
     * as well.
     * @param element
     *     the element to remove
     * @throws PoolException
     *     if the associated {@link #closeElement(Object)} method needs to be
     *     called, and fails
     */
    public void removeNow(T element) {
        throwIfClosed();
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
     * Remove an object now from the pool, if it is available.
     * <p>
     * This method should timeout after {@code millis} milliseconds.
     * An unchecked exception may be thrown
     * if the element is not present, but it may be silently ignored
     * as well.
     * @param element
     *     the element to remove
     * @param millis
     *     timeout in milliseconds
     * @throws InterruptedException
     *     if timed out
     * @throws PoolException
     *     if the associated {@link #closeElement(Object)} method needs to be
     *     called, and fails
     */
    public void removeNow(T element, long millis) throws InterruptedException {
        throwIfClosed();
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
        throwIfClosed();
        Whirlpool.EVICTION_SCHEDULER.addPoolToSchedule(this);
    }

    /**
     * Remove an object pool from eviction in background, if
     * it has been registered before.
     */
    public void removeFromEvictionSchedule() {
        throwIfClosed();
        Whirlpool.EVICTION_SCHEDULER.removePoolFromSchedule(this);
    }

    /* ******************************************************
     *                    public method                    *
     * *****************************************************/

    private void throwIfClosed() {
        if (closed.get()) {
            throw PoolException.poolClosed();
        }
    }

    private void evictHelper() {
        evictOrRemove(false);
    }

    private void removeHelper() {
        evictOrRemove(true);
    }

    private void evictHelper(T element) {
        evictOrRemoveNow(element, false);
    }

    private void removeNowHelper(T element) {
        evictOrRemoveNow(element, true);
    }

    private void evictOrRemove(boolean instantly) {
        int numEvicted = 0;
        val shiftedCurrentTimestamp = instantly
                ? 0
                : System.currentTimeMillis() - expirationTime;
        while (orderedExpiringObjects.size() > 0) {
            val head = orderedExpiringObjects.getFirst();
            if (instantly || expiring.get(head) <= shiftedCurrentTimestamp) {
                expiring.remove(head);
                orderedExpiringObjects.removeFirst();
                this.closeElement(head);
                numEvicted++;
            }
            else {
                break;
            }
        }
        if (numEvicted == 0) {
            return;
        }
        availableElements.addAndGet(-numEvicted);
        totalSize.addAndGet(-numEvicted);
        if (log.isDebugEnabled()) {
            val msg = String.format("evicted %d element(s), idle element(s): %d, total number: %d",
                    numEvicted, availableElements.get(), totalSize.get());
            log.debug(msg);
        }
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
                availableElements.getAndDecrement();
                totalSize.getAndDecrement();
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
        if (availableElements.get() > 0) {
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
            availableElements.getAndDecrement();
        }
        if (element == null) {
            element = createElement();
            totalSize.getAndIncrement();
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
        availableElements.getAndIncrement();
    }

    /* ******************************************************
     *                     static method                    *
     * *****************************************************/


}
