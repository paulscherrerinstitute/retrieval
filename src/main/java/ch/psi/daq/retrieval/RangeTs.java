package ch.psi.daq.retrieval;

import ch.psi.daq.retrieval.pod.api1.Range;

import java.time.Instant;
import java.time.ZonedDateTime;

public class RangeTs {
    long beg;
    long end;

    public RangeTs(long beg, long end) {
        this.beg = beg;
        this.end = end;
    }

    public RangeTs(Range range) {
        Instant i;
        i = ZonedDateTime.parse(range.startDate).toInstant();
        beg = 1000000000L * i.getEpochSecond() + i.getNano();
        i = ZonedDateTime.parse(range.endDate).toInstant();
        end = 1000000000L * i.getEpochSecond() + i.getNano();
    }

    public long beg() { return beg; }

    public long end() { return end; }

}
