package ch.psi.daq.retrieval;

import ch.psi.daq.retrieval.pod.api1.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBufferFactory;

import java.time.Instant;
import java.util.List;

public class QueryParams {
    static Logger LOGGER = LoggerFactory.getLogger("QueryParams");
    public List<String> channels;
    public String beginString;
    public String endString;
    public Instant begin;
    public Instant end;
    public long endNanos;
    public List<Integer> splits;
    public DataBufferFactory bufFac;
    public int bufferSize;
    public boolean decompressOnServer;
    public long limitBytes;
    public static QueryParams fromQuery(Query x, DataBufferFactory bufFac, int bufferSize) {
        QueryParams ret = new QueryParams();
        ret.bufFac = bufFac;
        ret.bufferSize = x.bufferSize > 0 ? x.bufferSize : bufferSize;
        ret.channels = x.channels;
        ret.beginString = x.range.startDate;
        ret.endString = x.range.endDate;
        ret.begin = Instant.parse(x.range.startDate);
        ret.end = Instant.parse(x.range.endDate);
        ret.endNanos = 1000000L * ret.end.toEpochMilli();
        ret.splits = x.splits == null ? List.of() : x.splits;
        ret.decompressOnServer = x.decompressOnServer == 1;
        ret.limitBytes = x.limitBytes;
        if (ret.begin.isAfter(ret.end)) {
            throw new IllegalArgumentException(String.format("Begin date %s is after end date %s", ret.begin, ret.end));
        }
        return ret;
    }
}
