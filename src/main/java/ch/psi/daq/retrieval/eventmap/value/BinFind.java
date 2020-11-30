package ch.psi.daq.retrieval.eventmap.value;

import ch.psi.daq.retrieval.pod.api1.Range;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

public class BinFind {
    public ZonedDateTime ts1;
    public ZonedDateTime ts2;
    public long off;
    public long dt;
    public int nBins;

    public BinFind(int nBins, Range range) {
        this.nBins = nBins;
        this.ts1 = ZonedDateTime.parse(range.startDate);
        this.ts2 = ZonedDateTime.parse(range.endDate);
        Instant ts1i = ts1.toInstant();
        this.off = 1000000000L * ts1i.getEpochSecond() + (long) ts1i.getNano();
        dt = 1000 * ChronoUnit.MICROS.between(ts1, ts2);
    }

    public int find(long ts) {
        if (ts < off) {
            throw new RuntimeException("timestamp smaller than start of range");
        }
        return (int) ((ts - off) / (dt / nBins));
    }

}
