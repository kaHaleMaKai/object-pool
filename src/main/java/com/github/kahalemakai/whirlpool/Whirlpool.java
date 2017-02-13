package com.github.kahalemakai.whirlpool;

import com.github.kahalemakai.tuples.Tuple;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.log4j.Log4j;
import lombok.val;

import java.lang.ref.ReferenceQueue;
import java.util.Deque;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Concurrent implementation of the {@link Poolable Poolable} interface.
 * <p>
 * Besides implementing the interface, periodic eviction of
 * expired elements in the object pool may be set up
 * by calling {@link #scheduleForEviction()}.
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
    private final Deque<Tuple<T, Long>> queue;
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
    private final AtomicBoolean evictionActive;
    @Getter
    private final boolean asyncClose;
    @Getter
    private final boolean asyncUnhand;
    @Getter
    private final boolean asyncFill;

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
                     boolean asyncFill) {
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
        evictionActive = new AtomicBoolean(false);
        this.minSize = minSize;
        this.maxSize = maxSize;
        for (int i = 0; i < minSize; ++i) {
            enqueue(createElement());
        }
        sufficiency.release(minSize);
        this.asyncClose = asyncClose;
        this.asyncUnhand = asyncUnhand;
        this.asyncFill = asyncFill;
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
                DEFAULT_ASYNC_FILL);
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
        val obj = takeHelper();
        afterBorrowing();
        return obj;
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
        if (!closed.get()) {
            necessity.release();
            if (asyncUnhand) {
                runAsync(() -> {
                    enqueue(element);
                    sufficiency.release();
                    if (0 < maxSize) {
                        while (maxSize < necessity.availablePermits()) {
                            closeElement(take());
                        }
                    }
                });
                return;
            }
            enqueue(element);
            sufficiency.release();
            if (0 < maxSize) {
                while (maxSize < necessity.availablePermits()) {
                    closeElement(take());
                }
            }
        }
        else {
            closeElement(element);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void unhand(T element, long millis) throws InterruptedException {
        unhand(element);
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
    public void evictAll() {
        throwIfClosed();
        cleanupReferences();
        fillToMinSize(false);
        val ts = System.currentTimeMillis() - expirationTime;
        while (true) {
            if (!(evictHead(ts))) {
                break;
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void evictAll(long millis) throws InterruptedException {
        throwIfClosed();
        cleanupReferences();
        fillToMinSize(false);
        val ts = System.currentTimeMillis() - expirationTime;
        while (true)
            if (!evictHead(ts, millis)) {
                break;
            }
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
                closeElement(head.first());
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
        if (asyncClose) {
            runAsync(() -> {
                synchronized (tracker) {
                    tracker.remove(element);
                }
                super.closeElement(element);
            });
            return;
        }
        synchronized (tracker) {
            tracker.remove(element);
        }
        super.closeElement(element);
    }

    @Override
    public T createElement() {
        val element = super.createElement();
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
        fillToMinSize(asyncFill);
    }

    private T borrowHelper() {
        boolean necessary = false;
        try {
            necessary = necessity.tryAcquire(0L, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.interrupted();
        }
        if (necessary) {
            try {
                sufficiency.acquire();
            } catch (InterruptedException e) {
                Thread.interrupted();
                necessity.release();
                return createElement();
            }
            val head = queue.poll();
            assert head != null;
            val obj = head.first();
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

    private T takeHelper() {
        Tuple<T, Long> head = null;
        boolean necessary = false;
        try {
            necessary = necessity.tryAcquire(0L, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.interrupted();
        }
        if (necessary) {
            boolean sufficient = false;
            try {
                sufficient = sufficiency.tryAcquire(0L, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                Thread.interrupted();
                necessity.release();
            }
            if (sufficient) {
                head = queue.poll();
            }
            else {
                necessity.release();
            }
            assert head != null;
            return head.first();
        }
        else {
            return createElement();
        }
    }

    private T borrowHelper(long millis) throws InterruptedException {
        Tuple<T, Long> head = null;
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
            return head.first();
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
        return head.first();
    }

    private boolean evictHead(long ts) {
        // prevent from running concurrently
        if (!evictionActive.compareAndSet(false, true)) {
            return false;
        }
        try {
            // no work to do
            if (necessity.availablePermits() <= this.minSize) {
                return false;
            }
            boolean sufficient;
            try {
                sufficient = sufficiency.tryAcquire(0L, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                Thread.interrupted();
                return false;
            }
            if (sufficient) {
                val head = queue.poll();
                if (head.last() <= ts) {
                    closeElement(head.first());
                    boolean necessary = false;
                    try {
                        necessary = necessity.tryAcquire(0L, TimeUnit.NANOSECONDS);
                    } catch (InterruptedException e) {
                        Thread.interrupted();
                    }
                    if (!necessary) {
                        if (asyncUnhand) {
                            runAsync(() -> {
                                enqueue(createElement());
                                sufficiency.release();
                            });
                            return true;
                        }
                        enqueue(createElement());
                        sufficiency.release();
                    }
                    return true;
                }
                else {
                    if (asyncUnhand) {
                        runAsync(() -> {
                            queue.addFirst(head);
                            sufficiency.release();
                        });
                        return false;
                    }
                    queue.addFirst(head);
                    sufficiency.release();
                }
            }
            return false;
        } finally {
            evictionActive.set(false);
        }
    }

    private boolean evictHead(long ts, long millis) {
        // prevent from running concurrently
        if (!evictionActive.compareAndSet(false, true)) {
            return false;
        }
        try {
            // no work to do
            if (necessity.availablePermits() <= this.minSize) {
                return false;
            }
            boolean sufficient;
            val start = System.nanoTime();
            val nanos = TimeUnit.MILLISECONDS.toNanos(millis);
            try {
                sufficient = sufficiency.tryAcquire(nanos, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                Thread.interrupted();
                return false;
            }
            if (sufficient) {
                val head = queue.poll();
                if (head.last() <= ts) {
                    closeElement(head.first());
                    boolean necessary = false;
                    val remainder = System.nanoTime() - start;
                    try {
                        necessary = necessity.tryAcquire(remainder, TimeUnit.NANOSECONDS);
                    } catch (InterruptedException e) {
                        Thread.interrupted();
                    }
                    if (!necessary) {
                        if (asyncUnhand) {
                            runAsync(() -> {
                                enqueue(createElement());
                                sufficiency.release();
                            });
                            return true;
                        }
                        enqueue(createElement());
                        sufficiency.release();
                    }
                    return true;
                }
                else {
                    if (asyncUnhand) {
                        runAsync(() -> {
                            queue.addFirst(head);
                            sufficiency.release();
                        });
                        return false;
                    }
                    queue.addFirst(head);
                    sufficiency.release();
                }
            }
            return false;
        } finally {
            evictionActive.set(false);
        }
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
        val t = Tuple.of(element, System.currentTimeMillis());
        queue.add(t);
    }

    private void fillToMinSize(boolean async) {
        if (minSize > 0 && necessity.availablePermits() < minSize && sufficiency.availablePermits() < minSize) {
            if (async) {
                runAsync(this::fillToMinSizeHelper);
                return;
            }
            fillToMinSizeHelper();
        }
    }

    private void fillToMinSizeHelper() {
        while (necessity.availablePermits() < minSize && sufficiency.availablePermits() < minSize) {
            necessity.release();
            enqueue(createElement());
            sufficiency.release();
        }
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
