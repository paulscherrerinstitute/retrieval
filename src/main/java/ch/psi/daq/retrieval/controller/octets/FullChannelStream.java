package ch.psi.daq.retrieval.controller.octets;

import ch.psi.daq.retrieval.merger.CompleteEvent;
import ch.psi.daq.retrieval.status.Error;
import ch.psi.daq.retrieval.utils.DateExt;
import ch.psi.daq.retrieval.QueryParams;
import ch.psi.daq.retrieval.reqctx.ReqCtx;
import ch.psi.daq.retrieval.bytes.BufCont;
import ch.psi.daq.retrieval.controller.QueryData;
import ch.psi.daq.retrieval.controller.mergefunc.MergeFunction;
import ch.psi.daq.retrieval.eventmap.value.TransformSupplier;
import ch.psi.daq.retrieval.merger.MergerSupport;
import ch.psi.daq.retrieval.pod.api1.Channel;
import ch.psi.daq.retrieval.subnodes.SubTools;
import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicInteger;

public class FullChannelStream {
    static Logger LOGGER = (Logger) LoggerFactory.getLogger(FullChannelStream.class.getSimpleName());

    public FullChannelStream(Channel channel, Instant beg, Instant end, QueryParams qp, TransformSupplier<BufCont> valuemapSup, MergeFunction mergeFunction, ReqCtx reqCtx, QueryData queryData) {
        this.channel = channel;
        this.beg = beg;
        this.end = end;
        this.begNext = beg;
        this.qp = qp;
        this.valuemapSup = valuemapSup;
        this.mergeFunction = mergeFunction;
        this.reqCtx = reqCtx;
        this.queryData = queryData;
    }

    public Flux<BufCont> untilEos() {
        Instant instMax = DateExt.toInstant(tsEmitMax);
        Instant from;
        if (instMax.isAfter(beg)) {
            from = instMax.plus(Duration.ofNanos(1));
            LOGGER.info("{}  untilEos  instMax as from:   {}", reqCtx, from.atZone(ZoneOffset.UTC).format(DateExt.datefmt));
        }
        else {
            from = beg;
            LOGGER.info("{}  untilEos  beg     as from:   {}", reqCtx, from.atZone(ZoneOffset.UTC).format(DateExt.datefmt));
        }
        long endNanos = DateExt.toLong(end);
        triggerFirst = true;
        AtomicInteger count = new AtomicInteger();
        return SubTools.buildMergedChannel(reqCtx, channel, qp, from, end, 1, mergeFunction, queryData.requestStatusBoard, queryData.conf)
        .flatMapMany(mc -> {
            return mc.fl
            .doOnError(e -> {
                LOGGER.error("{}  queryMerged ERROR  {}", reqCtx, e.toString());
                queryData.requestStatusBoard.getOrCreate(reqCtx).addError(new Error(e));
            })
            .transform(MergerSupport::eventsFromTokens)
            .doOnNext(k -> {
                if (triggerFirst) {
                    triggerFirst = false;
                    LOGGER.info("{}  first ts {}", reqCtx, k.ts);
                }
                if (k.ts >= endNanos) {
                    LOGGER.info("{}  got TS after END  {}  {}", reqCtx, DateExt.toInstant(endNanos), DateExt.toInstant(k.ts));
                    term.set(1);
                }
                tsEmitMax = Math.max(k.ts, tsEmitMax);
                pulseEmitMax = Math.max(k.pulse, pulseEmitMax);
                count.getAndAdd(1);
            })
            .doFinally(k -> {
                LOGGER.info("{}  untilEos  doFinally  count {}  tsEmitMax {}  {}", reqCtx, count.get(), tsEmitMax, DateExt.toInstant(tsEmitMax).atZone(ZoneOffset.UTC).format(DateExt.datefmt));
            })
            .concatMapIterable(CompleteEvent::take, 1)
            .transform(fl -> MergerSupport.flattenSlices(fl, endNanos, channel.name, qp, reqCtx.bufCtx))
            .transform(fl -> valuemapSup.trans(fl, channel, mc.configEntry))
            .doOnNext(k -> {
                int rb = k.readableByteCount();
                QueryData.totalBytesEmitted.getAndAdd(rb);
                reqCtx.addBodyLen(rb);
            });
        });
    }

    public boolean isTerm() {
        return term.get() != 0;
    }

    public Channel channel() {
        return channel;
    }

    AtomicInteger term = new AtomicInteger();
    Channel channel;
    Instant beg;
    Instant end;
    Instant begNext;
    QueryParams qp;
    TransformSupplier<BufCont> valuemapSup;
    MergeFunction mergeFunction;
    ReqCtx reqCtx;
    QueryData queryData;
    long tsEmitMax;
    long pulseEmitMax;
    boolean triggerFirst;
    public Duration delay = Duration.ZERO;
    public boolean firstInQueue;

}
