package ch.psi.daq.retrieval.finder;

public class TimeBin2 implements Comparable<TimeBin2> {
    public long timeBin;
    public long binSize;
    public boolean hasIndex;
    public TimeBin2(long timeBin, long binSize, boolean hasIndex) {
        this.timeBin = timeBin;
        this.binSize = binSize;
        this.hasIndex = hasIndex;
    }
    @Override
    public int compareTo(TimeBin2 x) {
        return Long.compare(timeBin * binSize, x.timeBin * x.binSize);
    }
}
