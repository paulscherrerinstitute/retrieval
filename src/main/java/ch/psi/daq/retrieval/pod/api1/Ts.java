package ch.psi.daq.retrieval.pod.api1;

public class Ts {
    public long sec;
    public long ns;
    public Ts(long ts) {
        sec = ts / 1000000000L;
        ns = ts % 1000000000L;
    }
}
