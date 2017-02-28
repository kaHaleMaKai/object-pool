package com.github.kahalemakai.whirlpool;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

class UnsafeManager {

    private static final Unsafe UNSAFE;
    static {
        UNSAFE = initUnsafe();
    }

    static Unsafe getUnsafe() {
        return UNSAFE;
    }

    static long getOffset(Class<?> clazz, String fieldName) {
        try {
            return UNSAFE.objectFieldOffset(
                    clazz.getDeclaredField(fieldName));
        } catch (NoSuchFieldException e) {
            throw new NullPointerException("could not get field offset for " + fieldName);
        }
    }

    private static Unsafe initUnsafe() {
        Field f = null;
        try {
            f = Unsafe.class.getDeclaredField("theUnsafe");
        } catch (NoSuchFieldException e) {
            throw new NullPointerException("could not initialize UNSAFE");
        }
        f.setAccessible(true);
        try {
            return (Unsafe) f.get(null);
        } catch (IllegalAccessException e) {
            throw new NullPointerException("could not initialize UNSAFE");
        }
    }

}
