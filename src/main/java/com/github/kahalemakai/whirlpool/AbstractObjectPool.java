package com.github.kahalemakai.whirlpool;

import com.github.kahalemakai.whirlpool.eviction.AutoClosing;
import lombok.Getter;
import lombok.extern.log4j.Log4j;
import lombok.val;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

@Log4j
abstract class AbstractObjectPool<T> implements Poolable<T> {

    private static final long SECOND = 1000;

    public static final long DEFAULT_EXPIRATION_TIME = 30 * SECOND;

    protected static final Supplier<?> DEFAULT_CREATE_FN;
    static {
        DEFAULT_CREATE_FN = () -> {
            throw new UnsupportedOperationException("missing Supplier to create elements");
        };
    }

    protected static final Predicate<?> DEFAULT_VALIDATION_FN;
    static {
        DEFAULT_VALIDATION_FN = (t) -> true;
    }

    protected static final Consumer<?> NO_OP_CONSUMER;
    static {
        NO_OP_CONSUMER = (t) -> { };
    }

    protected final AtomicInteger availableElements = new AtomicInteger(0);
    protected final AtomicInteger totalSize = new AtomicInteger(0);

    /**
     * @return {@inheritDoc}
     */
    @Getter
    protected final long expirationTime;

    private final Supplier<T> createFn;
    private final Predicate<T> validationFn;
    private final Consumer<T> closeFn;
    private final Consumer<T> prepareFn;
    private final Consumer<T> resetFn;

    protected final AtomicBoolean closed = new AtomicBoolean(false);

    public AbstractObjectPool(final long expirationTime,
                              final Supplier<T> onCreate,
                              final Predicate<T> onValidation,
                              final Consumer<T> onClose,
                              final Consumer<T> onPrepare,
                              final Consumer<T> onReset) {
        this.expirationTime = expirationTime;
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
            val msg = "created new element " + element;
            log.debug(msg);
        }
        return element;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AutoClosing<T> borrowKindly() {
        return AutoClosing.of(borrow(), this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AutoClosing<T> borrowKindly(long millis) throws InterruptedException {
        return AutoClosing.of(borrow(millis), this);
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

    /* ******************************************************
     *                   protected method                   *
     * *****************************************************/

    protected void throwIfClosed() {
        if (closed.get()) {
            throw PoolException.poolClosed();
        }
    }

}
