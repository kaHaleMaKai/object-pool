package com.github.kahalemakai.whirlpool;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class Mutator<T> {
    private T value;

    public T get() {
        return value;
    }

    public void set(T newValue) {
        this.value = newValue;
    }
}
