package com.github.kahalemakai.whirlpool;

/**
 * The {@code Poolable} defines an interface for object pools.
 * <p>
 * All direct interaction with the included objects come in two flavors:
 * <ul>
 *     <li>blocking: methods waiting indefinitely</li>
 *     <li>timed: methods interrupting after a timeout</li>
 * </ul>
 * , e.g. {@link #borrow()} and {@link #borrow(long)}.
 * <p>
 * The number of currently available elements (i.e. that can be borrowed
 * from the pool) can be retrieved via {@link #availableElements()},
 * and the total number of borrowed + available elements by calling
 * {@link #totalSize()}. Those methods should deliver accurate
 * numbers, which makes them blocking. If only an estimate is required,
 * it can be obtained by {@link #availableElementsEstimate()}, and
 * {@link #totalSizeEstimate()} respectively.
 *
 * @param <T>
 *     type of elements in the pool
 */
public interface Poolable<T> {

    /**
     * Borrow an object from the pool in a blocking way.
     * @return
     *     an object from the pool
     * @throws PoolException
     *    if an object needs to be created before
     *    handing it out, and the creation fails
     */
    T borrow();
    /**
     * Borrow an object from the pool and timeout after
     * {@code millis} milliseconds.
     * @param millis
     *     timeout in milliseconds
     * @return
     *     an object from the pool
     * @throws InterruptedException
     *     if timed out
     * @throws PoolException
     *    if an object needs to be created before
     *    handing it out, and the creation fails
     */
    T borrow(long millis) throws InterruptedException;

    /**
     * Return an object into the pool in a blocking fashion.
     * <p>
     * A pool should not accept objects it has not created
     * before. In those cases, an unchecked exception should
     * be thrown.
     * @param element
     *     the element to return
     */
    void unhand(T element);
    /**
     * Return an object into the pool and timeout after
     * {@code millis} milliseconds.
     * <p>
     * A pool should not accept objects it has not created
     * before. In those cases, an unchecked exception should
     * be thrown.
     * @param element
     *     the element to return
     * @param millis
     *     timeout in milliseconds
     * @throws InterruptedException
     *     if timed out
     */
    void unhand(T element, long millis) throws InterruptedException;

    /**
     * Evict all objects from the pool, that are currently not in use
     * and have expired.
     * <p>
     * This method should block.
     * @throws PoolException
     *     if a {@link #closeElement(Object)} method needs to be
     *     called, and fails
     */
    void evictAll();
    /**
     * Evict all objects from the pool, that are currently not in use
     * and have expired.
     * <p>
     * This method should timeout after {@code millis} milliseconds.
     *
     * @param millis
     *     timeout in milliseconds
     * @throws InterruptedException
     *     if timed out
     * @throws PoolException
     *     if a {@link #closeElement(Object)} method needs to be
     *     called, and fails
     */
    void evictAll(long millis) throws InterruptedException;

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
    void evict(T element);
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
    void evict(T element, long millis) throws InterruptedException;

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
    void removeNow(T element);
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
    void removeNow(T element, long millis) throws InterruptedException;

    /**
     * Get the precise number of elements that are available
     * for borrowing now.
     * <p>
     * This method should block. If only an estimate is
     * required, use the non-blocking method
     * {@link #availableElementsEstimate()}.
     * @return
     *     the total number of available elements
     *     in the pool
     */
    int availableElements();
    /**
     * Get an estimate on the number of elements that are
     * available for borrowing now.
     * <p>
     * To obtain the precise number of available elements
     * in a blocking way, use {@link #availableElements()}.
     * @return
     *     the total number of available elements
     *     in the pool
     */
    int availableElementsEstimate();

    /**
     * Get the precise number of currently existing
     * objects associated with the pool (borrowed +
     * available ones).
     * <p>
     * This method should block. If only an estimate is
     * required, use the non-blocking method
     * {@link #totalSizeEstimate()} ()}.
     * @return
     *     the total number of currently existing
     *     elements
     */
    int totalSize();
    /**
     * Get an estimate on the number of currently
     * existing objects associated with the pool
     * (borrowed + available ones).
     * <p>
     * To obtain the precise number of existing elements
     * (in a blocking way), use {@link #availableElements()}.
     * @return
     *     the total number of currently existing
     *     elements
     */
    int totalSizeEstimate();

    /**
     * Validate objects to be borrowed from the pool.
     * <p>
     * If an object fails to be validated, a new one
     * will be created instead by using the
     * {@link #createElement()} method.
     *
     * @param element
     *     the element to be validated
     * @return
     *     {@code true} if the object can be borrowed,
     *     else {@code false}
     */
    boolean validate(T element);

    /**
     * Instruct the {@code Poolable} how to construct
     * a new object, that may be borrowed.
     * @return
     *     a newly created object
     * @throws PoolException
     *    if object creation fails
     */
    T createElement();

    /**
     * Cleanup an object from the pool before removing it.
     * <p>
     * This method should only be implemented if releasing an
     * external resource is required on removing an object from
     * the pool.
     * @param element
     *     the object to close
     * @throws PoolException
     *    if closing the element fails
     */
    default void closeElement(T element) { }

    /**
     * Prepare an object's state so that it may be borrowed.
     * <p>
     * This method is <b>only</b> on borrowing an existing
     * object from the pool <b>after</b> successful validation.
     * It is <b>never</b> called on newly created objects.
     * <p>
     * Usually, it should suffice to either implement
     * this method or {@link #resetElement(Object)}.
     *
     * @param element
     *     the object to prepare
     */
    default void prepareElement(T element) { }

    /**
     * Reset the object's state on returning it to the pool.
     * <p>
     * This method is called on the object prior to adding
     * it to the pool's underlying {@code Collection}.
     * <p>
     * Usually, it should suffice to either implement
     * this method or {@link #prepareElement(Object)}:
     *
     * @param element
     *     the object to reset
     */
    default void resetElement(T element) { }

    /**
     * Get the time after which available (not borrowed)
     * objects in the pool will expire and may be evicted.
     * @return
     *     time after which objects expire
     */
    long getExpirationTime();

}
