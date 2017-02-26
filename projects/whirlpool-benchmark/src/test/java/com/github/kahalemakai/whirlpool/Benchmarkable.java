package com.github.kahalemakai.whirlpool;

public interface Benchmarkable {
    void close();
    boolean validate();
    void prepare();
    void reset();

    static boolean sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignore) { }
        return true;
    }

}
