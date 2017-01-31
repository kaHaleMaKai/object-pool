package com.github.kahalemakai.objectpool;

/**
 * The {@code ExceptionAction} interface is a variant
 * of the {@link java.util.function.Consumer} interface
 * that is allowed to throw checked exceptions.
 * <p>
 * This is a <a href="package-summary.html">functional interface</a>
 * whose functional method is {@link #accept(Object)}.
 *
 * @param <T>
 *     type of object to be acted on
 * @param <E>
 *     type of exception that might get thrown
 */
public interface ExceptionalConsumer<T, E extends Exception> {

    /**
     * Take an action on an object.
     * @param object
     *     the object to be acted on
     * @throws E
     *     exception that might get thrown
     */
    void accept(T object) throws E;

    ExceptionalConsumer<?, RuntimeException> NO_ACTION = (t) -> {};

    @SuppressWarnings("unchecked")
    static <S, X extends Exception> ExceptionalConsumer<S, X> noAction() {
        return (ExceptionalConsumer<S, X>) NO_ACTION;
    }
}
