package com.github.kahalemakai.whirlpool;

import lombok.Builder;
import lombok.Getter;
import lombok.extern.log4j.Log4j;
import lombok.val;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.Deque;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Concurrent implementation of the {@link Poolable Poolable} interface.
 * <p>
 * Besides implementing the interface, periodic eviction of
 * expired elements in the object pool is automatically carried out.
 * <p>
 * This class uses counting {@link Semaphore Semaphores} as synchronization guards.
 * Two are employed, one as a necessary condition for borrowing, and
 * another one as sufficient condition. On borrowing, both need to
 * be acquired in order to obtain an object from the pool. When
 * {@link #unhand(Object) unhanding} an object, the necessary one is
 * released on entry, and the sufficient one is only released after
 * completing the unhand operation. Under high workloads, only a small
 * amount of contraction should be experienced, but that has not been
 * tested yet.
 *
 * <h4>TODO</h4>
 *
 * <ul>
 *     <li>document all remaining methods</li>
 *     <li>benchmark</li>
 *     <li>add further tests</li>
 *     <li>add log output (especially on debug level)</li>
 * </ul>
 *
 * @param <T>
 *     type of objects in the pool
 */
@Log4j
public final class Whirlpool<T> extends AbstractObjectPool<T> {

    private static final boolean DEFAULT_THREAD_ACCESS_FAIRNESS = true;

    private final ReferenceQueue<T> refQueue;
    private final Map<T, Long> tracker;
    private final Deque<PoolEntry<T>> queue;
    @Getter
    private final int minSize;
    @Getter
    private final int maxSize;

    /**
     * Counting semaphore indicating the necessasry
     * condition for polling an element:
     * someone has tried to insert an element.
     */
    private final Semaphore necessity;

    /**
     * Counting semaphore indicating the sufficient
     * condition for polling an element:
     * an element was returned to the queue.
     */
    private final Semaphore sufficiency;
    @Getter
    private boolean asyncClose = true;
    @Getter
    private boolean asyncUnhand = true;
    @Getter
    private boolean asyncFill = true;
    @Getter
    private boolean asyncCreate = true;

    private final Scheduler scheduler;
    private int parallelism = 1;

    @Builder
    private Whirlpool(final long expirationTime,
                      final Supplier<T> onCreate,
                      final Predicate<T> onValidation,
                      final Consumer<T> onClose,
                      final Consumer<T> onPrepare,
                      final Consumer<T> onReset,
                      final boolean fairThreadAccess,
                      int minSize,
                      int maxSize,
                      boolean asyncClose,
                      boolean asyncUnhand,
                      boolean asyncFill,
                      boolean asyncCreate,
                      int parallelism) {
        super(expirationTime,
                onCreate,
                onValidation,
                onClose,
                onPrepare,
                onReset);
        if (maxSize <= 0) {
            maxSize = -1;
        }
        checkSizeArguments(minSize, maxSize);
        refQueue = new ReferenceQueue<>();
        tracker = new WeakHashMap<>();
        queue = new ConcurrentLinkedDeque<>();
        necessity = new Semaphore(minSize, fairThreadAccess);
        sufficiency = new Semaphore(0, fairThreadAccess);
        this.minSize = minSize;
        this.maxSize = maxSize;
        for (int i = 0; i < minSize; ++i) {
            enqueue(createElement());
        }
        sufficiency.release(minSize);
        this.asyncClose = asyncClose;
        this.asyncUnhand = asyncUnhand;
        this.asyncFill = asyncFill;
        this.asyncCreate = asyncCreate;
        this.parallelism = parallelism;
        this.scheduler = Scheduler.withThreads(parallelism);
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
                (Consumer<T>) NO_OP_CONSUMER,
                DEFAULT_THREAD_ACCESS_FAIRNESS,
                DEFAULT_MIN_SIZE,
                DEFAULT_MAX_SIZE,
                DEFAULT_ASYNC_CLOSE,
                DEFAULT_ASYNC_UNHAND,
                DEFAULT_ASYNC_FILL,
                false,
                1);
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
    public T borrow() {
        beforeBorrowing();
        val obj = borrowHelper();
        afterBorrowing();
        return obj;
    }

    public T take() {
        beforeBorrowing();
        val obj = takeOrNull();
        afterBorrowing();
        return obj == null ? createElement() : obj;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T borrow(long millis) throws InterruptedException {
        beforeBorrowing();
        val obj = borrowHelper(millis);
        afterBorrowing();
        return obj;
    }

    public T waitAndBorrow() throws InterruptedException {
        beforeBorrowing();
        val obj = waitAndBorrowHelper();
        afterBorrowing();
        return obj;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void unhand(T element) {
        boolean elementIsKnown = tracker.containsKey(element);
        // there will be no false positives, as keys are weak and
        // only removed when objects become garbage
        if (!elementIsKnown) {
            synchronized (tracker) {
                elementIsKnown = tracker.containsKey(element);
            }
        }
        if (!elementIsKnown) {
            val msg = "trying to unhand unknown element " + element;
            log.warn(msg);
            return;
        }
        if (closed.get()) {
            closeElement(element);
            return;
        }
        unhandElement(element);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void unhand(T element, long millis) throws InterruptedException {
        unhand(element);
    }

    private void unhandElement(T element) {
        sufficiency.release();
        if (asyncUnhand) {
            scheduler.submit(() -> {
                enqueue(element);
                necessity.release();
            });
            return;
        }
        enqueue(element);
        necessity.release();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int availableElements() {
        return necessity.availablePermits();
    }

    /**
     * {@inheritDoc}
     * This method delivers the same result as {@link #availableElements()}
     * due to the usage of counting {@link Semaphore Semaphores}
     */
    @Override
    public int availableElementsEstimate() {
        return availableElements();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int totalSize() {
        synchronized (tracker) {
            return tracker.size();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int totalSizeEstimate() {
        // FIXME this is neither appropriate, nor thread-safe
        return tracker.size();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws PoolException {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        // no more elements will be available
        // not really required, but semantically correct
        necessity.drainPermits();
        sufficiency.drainPermits();
        cleanupReferences();
        PoolException pex = null;
        while (true) {
            val head = queue.poll();
            if (head == null) {
                break;
            }
            try {
                closeElement(head.value());
            } catch (Exception e) {
                if (pex == null) {
                    pex = new PoolException("error while trying to close the Whirlpool");
                }
                log.warn(e.getMessage());
                pex.addSuppressed(e);
            }
        }
        if (pex != null) {
            log.error(pex);
            throw pex;
        }
    }

    @Override
    public void closeElement(T element) {
        synchronized (tracker) {
            tracker.remove(element);
        }
        if (asyncClose) {
            scheduler.closeElement(() -> super.closeElement(element));
            return;
        }
        super.closeElement(element);
    }

    @Override
    public T createElement() {
        T element = null;
        if (asyncCreate) {
            val future = scheduler.createElement(super::createElement);
            try {
                element = future.get();
            } catch (InterruptedException e) {
                Thread.interrupted();
            } catch (ExecutionException ignore) {
            } finally {
                if (element == null) {
                    element = super.createElement();
                }
            }

        }
        else {
            element = super.createElement();
        }
        synchronized (tracker) {
            tracker.putIfAbsent(element, System.currentTimeMillis());
        }
        return element;
    }

   /* *****************************************************
    *                package-private method               *
    * *****************************************************/

    void removeElement(PoolEntry<T> element) {
        if (!necessity.tryAcquire()) {
            return;
        }
        boolean removed = false;
        try {
            removed = queue.removeFirstOccurrence(element);
        } finally {
            if (!removed) {
                necessity.release();
            }
        }
        if (removed && !sufficiency.tryAcquire()) {
            enqueue(createElement());
        }
    }

    WeakReference<ScheduledFuture<?>> scheduleEviction(PoolEntry<T> entry) {
        return new WeakReference<>(scheduler.scheduleEviction(entry, expirationTime));
    }

   /* *****************************************************
    *                    private method                   *
    * *****************************************************/

   private boolean isNecessityGiven() {
       return isConditionGiven(necessity);
   }

    private boolean isSufficiencyGiven() {
       return isConditionGiven(sufficiency);
    }

    private boolean isConditionGiven(final Semaphore semaphore) {
        try {
            return semaphore.tryAcquire(0L, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.interrupted();
            return false;
        }
    }

    private void beforeBorrowing() {
        throwIfClosed();
    }

    private void afterBorrowing() {
    }

    private T borrowHelper() {
        if (isNecessityGiven()) {
            try {
                sufficiency.acquire();
            } catch (InterruptedException e) {
                Thread.interrupted();
                necessity.release();
                return createElement();
            }
            val head = queue.poll();
            assert head != null;
            val obj = head.value();
            if (!validate(obj)) {
                closeElement(obj);
                return createElement();
            }
            return obj;
        }
        else {
            return createElement();
        }
    }

    private T takeOrNull() {
        PoolEntry<T> head = null;
        if (isNecessityGiven()) {
            if (isSufficiencyGiven()) {
                head = queue.poll();
            }
            else {
                necessity.release();
            }
            assert head != null;
            return head.value();
        }
        return null;
    }

    private T borrowHelper(long millis) throws InterruptedException {
        PoolEntry<T> head = null;
        val nanos = TimeUnit.MILLISECONDS.toNanos(millis);
        val start = System.nanoTime();
        if (necessity.tryAcquire(nanos, TimeUnit.NANOSECONDS)) {
            val remainder = System.nanoTime() - start - nanos;
            boolean permitted;
            try {
                permitted = sufficiency.tryAcquire(remainder, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                necessity.release();
                throw e;
            }
            if (permitted) {
                head = queue.poll();
            }
            else {
                necessity.release();
            }
            assert head != null;
            return head.value();
        }
        else {
            return createElement();
        }
    }

    private T waitAndBorrowHelper() throws InterruptedException {
        necessity.acquire();
        try {
            sufficiency.acquire();
        } catch (InterruptedException e) {
            necessity.release();
            throw e;
        }
        val head = queue.poll();
        return head.value();
    }

    private void cleanupReferences() throws PoolException {
        if (log.isDebugEnabled()) {
            val msg = "cleaning up references";
            log.debug(msg);
        }
        PoolException pe = null;
        int numCleaned = 0;
        while (true) {
            val head = refQueue.poll();
            if (head == null) {
                break;
            }
            val object = head.get();
            if (object == null) {
                break;
            }
            try {
                closeElement(object);
                numCleaned++;
            }
            catch (Throwable e) {
                if (pe == null) {
                    pe = new PoolException("caught exception while cleaning up references");
                }
                log.info(e.getMessage());
                pe.addSuppressed(e);
            }
        }
        if (pe != null) {
            log.error("caught error while cleaning up reference", pe);
            throw pe;
        }
        if (log.isDebugEnabled()) {
            val msg = String.format("successfully cleaned up %d references", numCleaned);
            log.debug(msg);
        }
    }

    private void enqueue(T element) {
        val t = PoolEntry.of(element, expirationTime, this);
        queue.add(t);
    }

   /* *****************************************************
    *                private static method                *
    * *****************************************************/

    private static void checkSizeArguments(int minSize, int maxSize) throws IllegalArgumentException {
        // sanitize inputs
        if (minSize < 0) {
            val msg = String.format("wrong minSize. expected: minSize >= 0, got: %d", minSize);
            log.error(msg);
            throw new IllegalArgumentException(msg);
        }
        if (0 < maxSize && maxSize < minSize) {
            val msg = "wrong pool size. expected: minSize <= maxSize, got: minSize > maxSize";
            log.error(msg);
            throw new IllegalArgumentException(msg);
        }
    }

}
