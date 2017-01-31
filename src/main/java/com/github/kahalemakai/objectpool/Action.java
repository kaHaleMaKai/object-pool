package com.github.kahalemakai.objectpool;

/**
 * The {@code Action} interface is a semantically
 * reasonable substitution for {@link Runnable}, which
 * is too tightly linked to the {@link Thread} class.
 * <p>
 * This is a <a href="package-summary.html">functional interface</a>
 * whose functional method is {@link #act()}.
 *
 */
@FunctionalInterface
public interface Action {

    /**
     * Take a specified action.
     */
    void act();
}
