package ch.psi.daq.retrieval.utils;

import com.fasterxml.jackson.annotation.JsonGetter;

public class Microseconds {
    long mu;
    public static Microseconds fromNanos(long v) {
        Microseconds x = new Microseconds();
        x.mu = v / 1000;
        return x;
    }
    @JsonGetter("mu")
    public long mu() { return mu; }
}
