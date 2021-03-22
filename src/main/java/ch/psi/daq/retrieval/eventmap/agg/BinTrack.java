package ch.psi.daq.retrieval.eventmap.agg;

public class BinTrack {

    public BinTrack(int mode, long tsBeg, long pulseBeg, long tsEnd, long pulseEnd, int nrOfBins) {
        this.mode = mode;
        this.nrOfBins = nrOfBins;
        this.tsBeg = tsBeg;
        this.pulseBeg = pulseBeg;
        this.tsEnd = tsEnd;
        this.pulseEnd = pulseEnd;
        setMarkers();
    }

    public boolean isBefore(long ts, long pulse) {
        if (modePulse()) {
            throw new RuntimeException("todo");
        }
        else {
            if (ts < tsCurrent) {
                return true;
            }
            else {
                return false;
            }
        }
    }

    public boolean isAfter(long ts, long pulse) {
        if (modePulse()) {
            throw new RuntimeException("todo");
        }
        else {
            if (ts >= tsNext) {
                return true;
            }
            else {
                return false;
            }
        }
    }

    public boolean advance(long ts, long pulse) {
        if (modePulse()) {
            // TODO pulse
            throw new RuntimeException("TODO");
        }
        else {
            if (ts >= tsNext) {
                gotoNextBin();
                return true;
            }
            else {
                return false;
            }
        }
    }

    void setMarkers() {
        tsCurrent = tsBeg + (tsEnd - tsBeg) / nrOfBins * completedBins;
        pulseCurrent = pulseBeg + (pulseEnd - pulseBeg) / nrOfBins * completedBins;
        tsNext = tsEnd - (tsEnd - tsBeg) / nrOfBins * (nrOfBins - 1 - completedBins);
        pulseNext = pulseEnd + (pulseEnd - pulseBeg) / nrOfBins * (nrOfBins - 1 - completedBins);
    }

    public void gotoNextBin() {
        completedBins += 1;
        setMarkers();
    }

    public long currentBegin() {
        if (modePulse()) {
            throw new RuntimeException("TODO");
        }
        else {
            return tsCurrent;
        }
    }

    public long currentEnd() {
        if (modePulse()) {
            throw new RuntimeException("TODO");
        }
        else {
            return tsNext;
        }
    }

    public long lastBegin() {
        if (modePulse()) {
            throw new RuntimeException("TODO");
        }
        else {
            return tsBeg + (tsEnd - tsBeg) / nrOfBins * (completedBins - 1);
        }
    }

    public long lastEnd() {
        if (modePulse()) {
            throw new RuntimeException("TODO");
        }
        else {
            return tsCurrent;
        }
    }

    public boolean term() {
        if (completedBins >= nrOfBins) {
            return true;
        }
        if (modePulse()) {
            throw new RuntimeException("TODO");
        }
        else {
            return tsCurrent >= tsEnd;
        }
    }

    public boolean modeTs() {
        return mode == 0;
    }

    public boolean modePulse() {
        return mode == 1;
    }

    final int mode;
    final int nrOfBins;
    final long tsBeg;
    final long pulseBeg;
    final long tsEnd;
    final long pulseEnd;
    int completedBins;
    long tsCurrent;
    long pulseCurrent;
    long tsNext;
    long pulseNext;

}
