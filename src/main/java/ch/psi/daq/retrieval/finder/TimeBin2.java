package ch.psi.daq.retrieval.finder;

public class TimeBin2 implements Comparable<TimeBin2> {

    public TimeBin2(long timeBin, long binSize) {
        this.timeBin = timeBin;
        this.binSize = binSize;
    }

    @Override
    public int compareTo(TimeBin2 x) {
        return Long.compare(timeBin * binSize, x.timeBin * x.binSize);
    }

    public long timeBin;
    public long binSize;

}
