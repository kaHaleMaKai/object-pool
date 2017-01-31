package com.github.kahalemakai.objectpool;

/**
 * Define a {@link java.util.function.Supplier}-like interface
 * that allows for checked exceptions on calls to {@link #get()}.
 * <p>
 * This is a <a href="package-summary.html">functional interface</a>
 * whose functional method is {@link #get()}.
 *
 * @param <T> type of supplied element
 * @param <E> type of exception that might get thrown
 */
@FunctionalInterface
public interface ExceptionalSupplier<T, E extends Exception> {

    /**
     * Gets a result.
     * @return
     *     a result
     * @throws E
     *     exception that might get thrown
     *     on getting the result
     */
    T get() throws E;
}
