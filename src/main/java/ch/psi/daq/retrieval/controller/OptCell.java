package ch.psi.daq.retrieval.controller;

public class OptCell<T> {

    public static <T> OptCell<T> of(T k) {
        OptCell<T> ret = new OptCell<>();
        ret.inner = k;
        return ret;
    }

    public synchronized T take() {
        T ret = inner;
        inner = null;
        return ret;
    }

    public synchronized void put(T k) {
        inner = k;
    }

    T inner;

}
