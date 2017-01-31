package com.github.kahalemakai.objectpool;

/**
 * The {@code ExceptionAction} interface is a variant
 * of the {@link Action} interface that is allowed
 * to throw checked exceptions.
 * <p>
 * This is a <a href="package-summary.html">functional interface</a>
 * whose functional method is {@link #act()}.
 *
 * @param <E>
 *     type of exception that might get thrown
 */
public interface ExceptionalAction<E extends Exception> {

    /**
     * Take an action.
     * @throws E
     *     exception that might get thrown
     */
    void act() throws E;
}
