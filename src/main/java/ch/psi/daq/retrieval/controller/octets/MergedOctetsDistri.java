package ch.psi.daq.retrieval.controller.octets;

import ch.psi.daq.retrieval.QueryParams;
import ch.psi.daq.retrieval.config.Node;
import ch.psi.daq.retrieval.error.ErrorUtils;
import ch.psi.daq.retrieval.error.RequestException;
import ch.psi.daq.retrieval.merger.CompleteEvent;
import ch.psi.daq.retrieval.reqctx.ReqCtx;
import ch.psi.daq.retrieval.bytes.BufCont;
import ch.psi.daq.retrieval.controller.QueryData;
import ch.psi.daq.retrieval.controller.TransMapQueryMergedDefault;
import ch.psi.daq.retrieval.controller.TransMapQueryMergedFake;
import ch.psi.daq.retrieval.controller.mergefunc.MergeFunction;
import ch.psi.daq.retrieval.controller.mergefunc.MergeFunctionDefault;
import ch.psi.daq.retrieval.eventmap.value.TransformSupplier;
import ch.psi.daq.retrieval.merger.MergerSupport;
import ch.psi.daq.retrieval.pod.api1.Query;
import ch.psi.daq.retrieval.status.BufStatsCollector;
import ch.psi.daq.retrieval.status.Error;
import ch.psi.daq.retrieval.status.RequestStatus;
import ch.psi.daq.retrieval.status.RequestStatusFetch;
import ch.psi.daq.retrieval.subnodes.SubTools;
import ch.psi.daq.retrieval.throttle.Throttle;
import ch.psi.daq.retrieval.utils.rep1.PubRepeat;
import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.NettyDataBuffer;
import org.springframework.core.io.buffer.PooledDataBuffer;
import org.springframework.http.ResponseEntity;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

public class MergedOctetsDistri {
    static Logger LOGGER = (Logger) LoggerFactory.getLogger(MergedOctetsDistri.class.getSimpleName());


    // TODO  can I reuse this abstraction also for the other output formats?

    public Mono<ResponseEntity<Flux<DataBuffer>>> queryMergedOctets(ReqCtx reqCtx, QueryData queryData, Mono<Query> queryMono) {
        LOGGER.info("{}  queryMergedOctets", reqCtx);
        queryData.requestStatusBoard.requestBegin(reqCtx);
        Mono<Flux<DataBuffer>> mret = queryMono
        .doOnError(x -> LOGGER.error("{}  can not parse request", reqCtx))
        .map(query -> {
            QueryParams qp = QueryParams.fromQuery(queryData.conf, query, reqCtx.bufCtx);
            TransformSupplier<BufCont> valuemapSup;
            if (qp.valuemapType == 1) {
                valuemapSup = new TransMapQueryMergedFake(reqCtx, qp);
            }
            else {
                valuemapSup = new TransMapQueryMergedDefault(reqCtx, qp);
            }
            MergeFunction mergeFunction;
            if (qp.mergeType == 1) {
                mergeFunction = null;
            }
            else {
                mergeFunction = new MergeFunctionDefault();
            }
            //BufStatsCollector bsc1 = BufStatsCollector.create("MergedOctetsDistri::queryMergedOctets");
            //reqCtx.requestStatus().bufStatsCollectors.add(bsc1);
            return octetsForQuery(reqCtx, queryData, qp, valuemapSup, mergeFunction)
            //.transform(bsc1::apply)
            .transform(fl -> Throttle.throttleBufCont(fl, qp.throttleRate, qp.throttleSteps, qp.throttleInterval, qp.throttleOverslack))
            .transform(queryData.statusPing(reqCtx))
            .transform(QueryData.doDiscard("queryMergedOctetsFinal"))
            .map(k -> {
                if (k.closed()) {
                    LOGGER.error("{}  queryMergedOctets  closed BufCont while attempt to takeBuf", reqCtx);
                    return Optional.<DataBuffer>empty();
                }
                else {
                    return k.takeBuf();
                }
            })
            .filter(Optional::isPresent)
            .map(Optional::get)
            .doOnNext(k -> {
                if (k instanceof NettyDataBuffer) {
                    NettyDataBuffer q = (NettyDataBuffer) k;
                    int c = q.getNativeBuffer().refCnt();
                    if (c != 1) {
                        LOGGER.error("{}  unexpected refcount {}", reqCtx, c);
                    }
                }
                else if (k instanceof PooledDataBuffer) {
                    PooledDataBuffer q = (PooledDataBuffer) k;
                    LOGGER.warn("{}  unknown PooledDataBuffer  {}", reqCtx, q.getClass().getSimpleName());
                }
            })
            .doOnDiscard(Object.class, QueryData::doDiscardFinal)
            .doFinally(sig -> queryData.requestStatusBoard.bodyEmitted(reqCtx, sig));
        });
        return queryData.logResponse(reqCtx, "queryMergedOctets", mret);
    }

    Flux<BufCont> octetsForQuery(ReqCtx reqCtx, QueryData queryData, QueryParams qp, TransformSupplier<BufCont> valuemapSup, MergeFunction mergeFunction) {
        if (qp.waitForData > 0) {
            return octetsForQueryInterleaved(reqCtx, queryData, qp, valuemapSup, mergeFunction);
        }
        else {
            return octetsForQuerySeq(reqCtx, queryData, qp, valuemapSup, mergeFunction);
        }
    }

    Flux<BufCont> octetsForQuerySeq(ReqCtx reqCtx, QueryData queryData, QueryParams qp, TransformSupplier<BufCont> valuemapSup, MergeFunction mergeFunction) {
        LOGGER.info("{}  octetsForQuerySeq  {}  {}  {}  splits {}", reqCtx, qp.begin, qp.end, qp.channels, qp.splits);
        int channelPrefetch = 0;
        return Flux.fromIterable(qp.channels)
        .concatMap(channel -> {
            return SubTools.buildMergedChannel(reqCtx, channel, qp, qp.begin, qp.end, qp.trailingEventsMax, mergeFunction, queryData.requestStatusBoard, queryData.conf)
            .flatMapMany(mc -> {
                return mc.fl
                .doOnError(e -> {
                    LOGGER.error("{}  queryMerged ERROR  {}", reqCtx, e.toString());
                    queryData.requestStatusBoard.getOrCreate(reqCtx).addError(new Error(e));
                })
                .transform(MergerSupport::eventsFromTokens)
                .concatMapIterable(CompleteEvent::take, 1)
                .transform(fl -> MergerSupport.flattenSlices(fl, qp.endNanos, channel.name, qp, reqCtx.bufCtx))
                .transform(fl -> valuemapSup.trans(fl, channel, mc.configEntry))
                .doOnNext(k -> {
                    int rb = k.readableByteCount();
                    QueryData.totalBytesEmitted.getAndAdd(rb);
                    reqCtx.addBodyLen(rb);
                })
                .concatWith(Mono.defer(() -> {
                    return Flux.range(0, mc.subReqIds.size())
                    .concatMap(ix -> {
                        String id = mc.subReqIds.remove();
                        Node node = mc.subNodes.remove();
                        return RequestStatusFetch.getRequestStatus(queryData.requestStatusBoard, reqCtx, node.host, node.port, id)
                        .doOnNext(r -> {
                            if (r.isError()) {
                                LOGGER.error("{}  could not fetch status  {}  {}   {}", reqCtx, node, channel, r.error());
                            }
                            else {
                                RequestStatus st = r.status();
                                if (st.errors != null && st.errors.size() > 0) {
                                    LOGGER.error("{}  got errors count {}  from {}  {}", reqCtx, st.errors.size(), node, channel);
                                }
                            }
                        });
                    }, 0)
                    .then(Mono.empty());
                    //return Mono.empty();
                }))
                ;
            })
            ;
        }, channelPrefetch)
        //.transform(fl -> Flux.from(new ch.psi.daq.retrieval.utils.rep1.PubRepeat<>(fl, "octetsForQuerySeq")))
        ;
    }

    Flux<BufCont> octetsForQueryInterleaved(ReqCtx reqCtx, QueryData queryData, QueryParams qp, TransformSupplier<BufCont> valuemapSup, MergeFunction mergeFunction) {
        LOGGER.info("{}  octetsForQueryInterleaved  {}  {}  {}  splits {}", reqCtx, qp.begin, qp.end, qp.channels, qp.splits);
        ConcurrentLinkedQueue<FullChannelStream> cqueue = qp.channels.stream().map(channel -> {
            return new FullChannelStream(channel, qp.begin, qp.end, qp, valuemapSup, mergeFunction, reqCtx, queryData);
        })
        .collect(Collectors.toCollection(ConcurrentLinkedQueue::new));
        {
            FullChannelStream fc = cqueue.peek();
            if (fc != null) {
                fc.firstInQueue = true;
            }
        }
        return Flux.range(0, 200)
        .takeWhile(k -> {
            if (cqueue.size() <= 0) {
                LOGGER.info("{}  abort sub rr retries queue empty", reqCtx);
                return false;
            }
            else if (k > 0 && qp.end.plus(Duration.ofSeconds(qp.waitForData)).isBefore(Instant.now())) {
                LOGGER.info("{}  abort sub rr retries because timeout", reqCtx);
                return false;
            }
            else {
                return true;
            }
        })
        .doOnNext(k -> LOGGER.info("{}  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~  next sub attempt {}", reqCtx, k))
        .concatMap(k -> {
            FullChannelStream fc = cqueue.poll();
            if (fc != null) {
                if (!fc.isTerm()) {
                    Duration dur = fc.delay;
                    if (fc.firstInQueue) {
                        fc.delay = Duration.ofMillis(2000);
                    }
                    cqueue.add(fc);
                    return Mono.just(0)
                    .delayElement(dur)
                    .flatMapMany(q -> {
                        return fc.untilEos();
                    })
                    ;
                }
                else {
                    return Mono.empty();
                }
            }
            else {
                return Mono.empty();
            }
        }, 0)
        ;
    }

}
