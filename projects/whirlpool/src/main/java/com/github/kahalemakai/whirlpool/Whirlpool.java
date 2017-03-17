package com.github.kahalemakai.whirlpool;

import lombok.*;
import lombok.experimental.Accessors;
import lombok.extern.log4j.Log4j;

import java.lang.ref.ReferenceQueue;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
 *     <li>setIfAbsent further tests</li>
 *     <li>setIfAbsent log output (especially on debug level)</li>
 * </ul>
 *
 * @param <T>
 *     type of objects in the pool
 */
@Log4j
public final class Whirlpool<T> extends AbstractObjectPool<T> {

    public static final boolean DEFAULT_ASYNC_CREATE = false;
    public static final boolean DEFAULT_ASYNC_CLOSE = true;
    public static final boolean DEFAULT_ASYNC_UNHAND = false;
    public static final boolean DEFAULT_ASYNC_FILL = true;
    public static final int DEFAULT_MIN_SIZE = 0;
    public static final int INFINITE_MAX_SIZE = -1;
    public static final int DEFAULT_MAX_SIZE = INFINITE_MAX_SIZE;
    public static final long INFINITE_EXPIRATION_TIME = -1L;
    public static final int DEFAULT_PARALLELISM = 1;
    public static final boolean DEFAULT_THREAD_ACCESS_FAIRNESS = true;

    private final ReferenceQueue<T> refQueue;
    private final Map<T, Long> tracker;
    private final RingBuffer<T> buffer;
    @Getter
    private final int minSize;
    @Getter
    private final int maxSize;

    @Getter
    private final boolean asyncClose;
    @Getter
    private final boolean asyncUnhand;
    @Getter
    private final boolean asyncFill;
    @Getter
    private final boolean asyncCreate;

    private final int parallelism;

    private final Timer scheduler;
    private final AtomicBoolean evictionIsActive;
    private volatile long currentTimestamp;
    private final AtomicInteger unsucessfulEvictionsCounter;

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
        validateInput(expirationTime,
                onCreate,
                minSize,
                maxSize,
                asyncClose,
                asyncUnhand,
                asyncFill,
                asyncCreate,
                parallelism);
        refQueue = new ReferenceQueue<>();
        tracker = new WeakHashMap<>();
        this.minSize = minSize;
        this.maxSize = maxSize;
        this.asyncClose = asyncClose;
        this.asyncUnhand = asyncUnhand;
        this.asyncFill = asyncFill;
        this.asyncCreate = asyncCreate;
        this.parallelism = parallelism;
        this.evictionIsActive = new AtomicBoolean();
        this.scheduler = new Timer("eviction-scheduler", true);
        this.scheduler.scheduleAtFixedRate(
                EvictionTask.forPool(this),
                expirationTime,
                expirationTime
        );
        this.currentTimestamp = 0L;
        this.unsucessfulEvictionsCounter = new AtomicInteger();
        this.buffer = new RingBuffer<>(this.maxSize);
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

    public static <T> WhirlpoolBuilder<T> builder() {
        return new WhirlpoolBuilder<T>();
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
//        if (asyncUnhand) {
//            scheduler.submit(() -> {
//                enqueue(element);
//                sufficiency.release();
//                necessity.release();
//            });
//            return;
//        }
        enqueue(element);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int availableElements() {
        return buffer.size();
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
        cleanupReferences();
        PoolException pex = null;
        while (true) {
            val head = buffer.take();
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
//        if (asyncClose) {
//            scheduler.closeElement(() -> super.closeElement(element));
//            return;
//        }
        super.closeElement(element);
    }

    @Override
    public T createElement() {
        T element = null;
//        if (asyncCreate) {
//            val future = scheduler.createElement(super::createElement);
//            try {
//                element = future.blockAndGet();
//            } catch (InterruptedException e) {
//                Thread.interrupted();
//            } catch (ExecutionException ignore) {
//            } finally {
//                if (element == null) {
//                    element = super.createElement();
//                }
//            }
//
//        }
//        else {
            element = super.createElement();
//        }
        synchronized (tracker) {
            tracker.putIfAbsent(element, System.currentTimeMillis());
        }
        return element;
    }

   /* *****************************************************
    *                    private method                   *
    * *****************************************************/

    private void beforeBorrowing() {
        throwIfClosed();
    }

    private void afterBorrowing() {
    }

    private T borrowHelper() {
            val head = buffer.take();
            if (head == null) {
                return createElement();
            }
            if (!head.tryMarkAsUsed()) {
                System.out.println(head);
                return take();
            }
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
                head = buffer.take();
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
                head = buffer.take();
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
        val head = buffer.take();
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
        buffer.add(t);
    }

    private void triggerEviction() {
        if (!evictionIsActive.compareAndSet(false, true)) {
            return;
        }
        try {
            val dueTimestamp = System.currentTimeMillis();
            if (dueTimestamp < currentTimestamp) {
                return;
            }
            while (true) {
                if (!evictHead(dueTimestamp)) {
                    break;
                }
            }
        } finally {
            evictionIsActive.set(false);
        }
    }

    // TODO give element back to buffer if not expired
    private boolean evictHead(long dueTimestamp) {
        if (necessity.availablePermits() == 0 || sufficiency.availablePermits() == 0) {
            return false;
        }
        val peekOfHead = buffer.peek();
        long timestamp;
        if (peekOfHead == null) {
            return false;
        }
        timestamp = peekOfHead.expirationTimestamp();
        if (dueTimestamp < timestamp) {
            this.currentTimestamp = timestamp;
            return false;
        }
        if (necessity.availablePermits() == 0 || sufficiency.availablePermits() == 0) {
            return false;
        }
        if (!isNecessityGiven()) {
            this.currentTimestamp = timestamp;
            return false;
        }
        if (!isSufficiencyGiven()) {
            necessity.release();
            this.currentTimestamp = timestamp;
            return false;
        }
        val head = buffer.take();
        if (head == null) {
            necessity.release();
            sufficiency.release();
            this.currentTimestamp = timestamp;
            return false;
        }
        if (peekOfHead != head) {
            timestamp = head.expirationTimestamp();
            if (dueTimestamp < timestamp) {
                necessity.release();
                buffer.addFirst(head);
                sufficiency.release();
                this.currentTimestamp = timestamp;
                return false;
            }
        }
        if (!head.tryMarkAsUsed()) {
            necessity.release();
            buffer.addFirst(head);
            sufficiency.release();
            this.currentTimestamp = timestamp;
            return false;
        }
        val element = head.value();
        // TODO async closing
        closeElement(element);
        this.currentTimestamp = timestamp;
        return true;
    }

   /* *****************************************************
    *                private static method                *
    * *****************************************************/

    private static <S> void validateInput(final long expirationTime,
                                          final Supplier<S> onCreate,
                                          int minSize,
                                          int maxSize,
                                          boolean asyncClose,
                                          boolean asyncUnhand,
                                          boolean asyncFill,
                                          boolean asyncCreate,
                                          int parallelism) {
        if (onCreate == null) {
            val msg = "onCreate() must be defined";
            log.error(msg);
            throw new IllegalStateException(msg);
        }
        if (minSize < 0) {
            val msg = "wrong pool size given. expected: minSize >= 0, got: " + minSize;
            log.error(msg);
            throw new IllegalStateException(msg);
        }
        if (maxSize <=0 && maxSize != DEFAULT_MAX_SIZE) {
            val msg = "wrong pool size given. expected: maxSize > 0, got: " + maxSize;
            log.error(msg);
            throw new IllegalStateException(msg);
        }
        else if (0 < maxSize && maxSize < minSize) {
            val msg = "wrong pool size. expected: minSize <= maxSize, got: minSize > maxSize";
            log.error(msg);
            throw new IllegalStateException(msg);
        }
        if (expirationTime <= 0 && expirationTime != INFINITE_EXPIRATION_TIME) {
            val msg = "wrong expiration time given. expected: expirationTime > 0, got: " + expirationTime;
            log.error(msg);
            throw new IllegalStateException(msg);
        }
        if (parallelism < 0) {
            val msg = "wrong parallelism given. expected: parallelism >= 0, got: " + expirationTime;
            log.error(msg);
            throw new IllegalStateException(msg);
        }
        if (parallelism == 0
                && (expirationTime != INFINITE_EXPIRATION_TIME
                || asyncCreate || asyncUnhand || asyncClose || asyncFill)) {
            val msg = "argument mismatch. demanding async work, but setting parallelism to 0";
            log.error(msg);
            throw new IllegalStateException(msg);
        }
    }

    @Accessors(fluent = true, chain = true)
    @Getter @Setter
    @ToString
    @Log4j
    public static class WhirlpoolBuilder<T> {
        private long expirationTime;
        private @NonNull Supplier<T> onCreate;
        private @NonNull Predicate<T> onValidation;
        private @NonNull Consumer<T> onClose;
        private @NonNull Consumer<T> onPrepare;
        private @NonNull Consumer<T> onReset;
        private boolean fairThreadAccess = DEFAULT_THREAD_ACCESS_FAIRNESS;
        private int minSize = DEFAULT_MIN_SIZE;
        private int maxSize = DEFAULT_MAX_SIZE;
        private boolean asyncClose = DEFAULT_ASYNC_CLOSE;
        private boolean asyncUnhand = DEFAULT_ASYNC_UNHAND;
        private boolean asyncFill = DEFAULT_ASYNC_FILL;
        private boolean asyncCreate = DEFAULT_ASYNC_CREATE;
        private int parallelism = DEFAULT_PARALLELISM;

        WhirlpoolBuilder() { }

        /**
         * If called, objects in the pool will not expire, ever.
         * @return
         *     {@code this WhirlpoolBuilder} instance
         */
        public WhirlpoolBuilder<T> withoutExpiration() {
            expirationTime = INFINITE_EXPIRATION_TIME;
            return this;
        }

        public Whirlpool<T> build() {
            return new Whirlpool<>(
                    expirationTime,
                    onCreate,
                    onValidation,
                    onClose,
                    onPrepare,
                    onReset,
                    fairThreadAccess,
                    minSize,
                    maxSize,
                    asyncClose,
                    asyncUnhand,
                    asyncFill,
                    asyncCreate,
                    parallelism);
        }

    }

    @RequiredArgsConstructor(staticName = "forPool")
    private static class EvictionTask extends TimerTask {

        private final Whirlpool<?> pool;

        @Override
        public void run() {
            pool.triggerEviction();
        }

    }

}
