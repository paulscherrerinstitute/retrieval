package ch.psi.daq.retrieval.config;

public class ChannelConfigEntry {
    public long ts;
    public int ks;
    public long bs;
    public int sc;

    @Override
    public String toString() {
        return String.format("ChannelConfigEntry  ts %20d  ks %2d  bs %6d  sc %10d", ts, ks, bs, sc);
    }

}
