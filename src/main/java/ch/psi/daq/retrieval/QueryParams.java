package ch.psi.daq.retrieval;

import ch.psi.daq.retrieval.pod.api1.Query;
import org.springframework.core.io.buffer.DataBufferFactory;

import java.time.Instant;
import java.util.List;

public class QueryParams {
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
    public long limitEventsPerChannel;
    public long limitBytes;
    public long limitBytesPerChannel;
    public long throttleRate;
    public int throttleSteps;
    public int throttleInterval;
    public int throttleOverslack;
    public int subreqType;
    public int valuemapType;
    public int tsmapType;
    public int mergeType;
    public int prepareSubfluxPrefetch;
    public int subTokenRate;
    public int flattenSlicesPrefetch;
    public int mergerSupportItemVecPrefetch;
    public static QueryParams fromQuery(Query x, DataBufferFactory bufFac, int bufferSize) {
        QueryParams ret = new QueryParams();
        ret.bufFac = bufFac;
        ret.bufferSize = x.bufferSize > 0 ? x.bufferSize : bufferSize;
        ret.channels = x.channels;
        ret.beginString = x.range.startDate;
        ret.endString = x.range.endDate;
        ret.begin = Instant.parse(x.range.startDate);
        ret.end = Instant.parse(x.range.endDate);
        ret.endNanos = 1000L * 1000 * ret.end.toEpochMilli();
        ret.splits = x.splits;
        ret.decompressOnServer = x.decompressOnServer == 1;
        ret.limitEventsPerChannel = x.limitEventsPerChannel;
        ret.limitBytes = x.limitBytes;
        ret.limitBytesPerChannel = x.limitBytesPerChannel;
        ret.throttleRate = x.throttleRate > 0 ? x.throttleRate : 1024 * 1024 * 512;
        ret.throttleSteps = x.throttleSteps;
        ret.throttleInterval = x.throttleInterval;
        ret.throttleOverslack = x.throttleOverslack;
        ret.subreqType = x.subreqType;
        ret.valuemapType = x.valuemapType;
        ret.tsmapType = x.tsmapType;
        ret.mergeType = x.mergeType;
        ret.prepareSubfluxPrefetch = x.prepareSubfluxPrefetch;
        ret.subTokenRate = x.subTokenRate;
        ret.flattenSlicesPrefetch = x.flattenSlicesPrefetch;
        ret.mergerSupportItemVecPrefetch = x.mergerSupportItemVecPrefetch;
        if (ret.begin.isAfter(ret.end)) {
            throw new IllegalArgumentException(String.format("Begin date %s is after end date %s", ret.begin, ret.end));
        }
        return ret;
    }
}
