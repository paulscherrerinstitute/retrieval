package ch.psi.daq.retrieval;

import ch.psi.daq.retrieval.config.ConfigurationRetrieval;
import ch.psi.daq.retrieval.pod.api1.Aggregation;
import ch.psi.daq.retrieval.pod.api1.Channel;
import ch.psi.daq.retrieval.pod.api1.Query;
import ch.psi.daq.retrieval.reqctx.BufCtx;
import ch.psi.daq.retrieval.utils.DateExt;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

public class QueryParams {
    public List<Channel> channels;
    public String beginString;
    public String endString;
    public Instant begin;
    public Instant end;
    public long begNanos;
    public long endNanos;
    public Aggregation aggregation;
    public List<Integer> splits;
    public boolean decompressOnServer;
    public long limitEventsPerChannel;
    public long limitBytes;
    public long limitBytesPerChannel;
    public int errorAfterBytes;
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
    public int rawItemVecPrefetch;
    public int subwgroup;
    public int jsonOuterPrefetch;
    public int waitForData;
    public int trailingEventsMax;
    public int limitRawRead;
    public int bufferSizeDiskRead;

    public static QueryParams fromQuery(ConfigurationRetrieval conf, Query x, BufCtx bufCtx) {
        QueryParams ret = new QueryParams();
        ret.channels = x.channels.channels.stream().filter(k -> {
            return k.backend == null || k.backend.equals(conf.backend);
        })
        .collect(Collectors.toList());
        ret.beginString = x.range.startDate;
        ret.endString = x.range.endDate;
        ret.begin = Instant.parse(x.range.startDate);
        ret.end = Instant.parse(x.range.endDate);
        ret.begNanos = DateExt.toLong(ret.begin);
        ret.endNanos = DateExt.toLong(ret.end);
        ret.aggregation = x.aggregation;
        ret.splits = x.splits;
        ret.decompressOnServer = x.decompressOnServer == 1;
        ret.limitEventsPerChannel = x.limitEventsPerChannel;
        ret.limitBytes = x.limitBytes;
        ret.limitBytesPerChannel = x.limitBytesPerChannel;
        ret.errorAfterBytes = x.errorAfterBytes;
        if (conf.mergeLocal) {
            ret.throttleRate = 1024 * 1024 * 420;
        }
        else {
            ret.throttleRate = 1024 * 1024 * 120;
        }
        if (x.throttleRate > 0) {
            ret.throttleRate = x.throttleRate;
        }
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
        ret.rawItemVecPrefetch = x.rawItemVecPrefetch;
        ret.subwgroup = x.subwgroup;
        ret.waitForData = x.waitForData;
        ret.trailingEventsMax = x.trailingEventsMax;
        ret.limitRawRead = Math.max(Math.min(x.limitRawRead, 1024 * 1024 * 80), 0);
        if (conf.mergeLocal) {
            ret.bufferSizeDiskRead = 1024 * 32;
        }
        else {
            ret.bufferSizeDiskRead = 1024 * 4;
        }
        if (x.bufferSizeDiskRead > 0) {
            ret.bufferSizeDiskRead = Math.max(Math.min(x.bufferSizeDiskRead, 1024 * 256), 3);
        }
        if (ret.begin.isAfter(ret.end)) {
            throw new IllegalArgumentException(String.format("Begin date %s is after end date %s", ret.begin, ret.end));
        }
        return ret;
    }

}
