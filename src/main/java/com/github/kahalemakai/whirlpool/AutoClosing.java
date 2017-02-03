package com.github.kahalemakai.whirlpool;

import lombok.RequiredArgsConstructor;

import java.io.Closeable;
import java.io.IOException;

@RequiredArgsConstructor(staticName = "of")
public class AutoClosing<T> implements Closeable {
    private final T wrappedObject;
    private final Poolable<T> pool;

    @Override
    public void close() throws IOException {
        pool.unhand(wrappedObject);
    }

    public T get() {
        return wrappedObject;
    }
}
