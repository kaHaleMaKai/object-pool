package com.github.kahalemakai.whirlpool;

@FunctionalInterface
public interface CreateElementTask<T> {
    T createElement();
}
