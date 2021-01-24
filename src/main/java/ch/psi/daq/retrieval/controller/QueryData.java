package ch.psi.daq.retrieval.controller;

import ch.psi.daq.retrieval.BufCtx;
import ch.psi.daq.retrieval.ChannelEventStream;
import ch.psi.daq.retrieval.KeyspaceToDataParams;
import ch.psi.daq.retrieval.MapFunctionFactory;
import ch.psi.daq.retrieval.PositionedDatafile;
import ch.psi.daq.retrieval.QueryParams;
import ch.psi.daq.retrieval.ReqCtx;
import ch.psi.daq.retrieval.bytes.BufCont;
import ch.psi.daq.retrieval.config.ConfigurationRetrieval;
import ch.psi.daq.retrieval.eventmap.ts.EventBlobToV1MapTs;
import ch.psi.daq.retrieval.eventmap.ts.MapTsItemVec;
import ch.psi.daq.retrieval.eventmap.ts.MapTsToken;
import ch.psi.daq.retrieval.eventmap.value.AggFunc;
import ch.psi.daq.retrieval.eventmap.value.AggMapper;
import ch.psi.daq.retrieval.eventmap.value.AggMax;
import ch.psi.daq.retrieval.eventmap.value.AggMean;
import ch.psi.daq.retrieval.eventmap.value.AggMin;
import ch.psi.daq.retrieval.eventmap.value.AggSum;
import ch.psi.daq.retrieval.eventmap.value.BinFind;
import ch.psi.daq.retrieval.eventmap.value.EventBlobMapResult;
import ch.psi.daq.retrieval.eventmap.value.EventBlobToV1Map;
import ch.psi.daq.retrieval.eventmap.value.TransformSupplier;
import ch.psi.daq.retrieval.finder.BaseDirFinderFormatV0;
import ch.psi.daq.retrieval.finder.TimeBin2;
import ch.psi.daq.retrieval.merger.MergerSupport;
import ch.psi.daq.retrieval.merger.Releasable;
import ch.psi.daq.retrieval.pod.api1.Query;
import ch.psi.daq.retrieval.status.RequestStatus;
import ch.psi.daq.retrieval.status.RequestStatusBoard;
import ch.psi.daq.retrieval.throttle.Throttle;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.turbo.TurboFilter;
import ch.qos.logback.core.spi.FilterReply;
import io.netty.buffer.ByteBuf;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.core.io.buffer.LimitedDataBufferList;
import org.springframework.core.io.buffer.NettyDataBuffer;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

public class QueryData {
    static Logger LOGGER = (Logger) LoggerFactory.getLogger(QueryData.class.getSimpleName());
    public BaseDirFinderFormatV0 baseDirFinder;
    AtomicLong nFilteredEmptyRawOut = new AtomicLong();
    ConfigurationRetrieval conf;
    static final String HEADER_X_DAQBUFFER_REQUEST_ID = "x-daqbuffer-request-id";
    static final String HEADER_X_CANONICALHOSTNAME = "X-CanonicalHostname";
    public static AtomicLong totalBytesEmitted = new AtomicLong();
    public static AtomicLong indexSizeSmall = new AtomicLong();
    public static AtomicLong indexSizeMedium = new AtomicLong();
    public static AtomicLong indexSizeLarge = new AtomicLong();
    public static AtomicLong indexSizeHuge = new AtomicLong();
    public final RequestStatusBoard requestStatusBoard = new RequestStatusBoard();
    public int port;

    static Marker logMarkerWebClientResponse = MarkerFactory.getMarker("WebClientResponse");
    static Marker logMarkerWebClientResponseItem = MarkerFactory.getMarker("WebClientResponseItem");
    static Marker logMarkerQueryMergedItems = MarkerFactory.getMarker("QueryMergedItems");
    static Marker logMarkerRawLocalItem = MarkerFactory.getMarker("RawLocalItem");
    static Marker logMarkerAccept = MarkerFactory.getMarker("Accept");

    static class ItemFilter extends TurboFilter {
        @Override
        public FilterReply decide(Marker marker, Logger logger, Level level, String format, Object[] objs, Throwable err) {
            if (objs != null && objs.length >= 1) {
                Object o0 = objs[0];
                if (o0 instanceof ReqCtx) {
                    ReqCtx ctx = (ReqCtx) o0;
                    if (ctx.logLevel != null && level.isGreaterOrEqual(ctx.logLevel)) {
                        return FilterReply.ACCEPT;
                    }
                }
            }
            if (marker != null) {
                if (marker.equals(logMarkerRawLocalItem)) {
                    return FilterReply.DENY;
                }
                if (marker.equals(logMarkerWebClientResponseItem)) {
                    return FilterReply.DENY;
                }
                if (marker.equals(logMarkerAccept)) {
                    return FilterReply.ACCEPT;
                }
            }
            return FilterReply.NEUTRAL;
        }
    }

    static {
        LOGGER.getLoggerContext().addTurboFilter(new ItemFilter());
    }

    static class Timeout1 extends RuntimeException {}
    static class Timeout2 extends RuntimeException {}
    static class Timeout3 extends RuntimeException {}

    public static Scheduler clientsplitnodeiter = Schedulers.newParallel("csn", 16);

    public QueryData(BaseDirFinderFormatV0 finder, ConfigurationRetrieval conf) {
        this.baseDirFinder = finder;
        this.conf = conf;
    }

    long cleanCount = 0;
    public void scheduledStatusClean() {
        if (cleanCount == Long.MAX_VALUE) {
            cleanCount = 0;
        }
        else {
            cleanCount += 1;
        }
        long n1 = requestStatusBoard.gc();
        if (n1 > 0) {
            cleanCount = 0;
            LOGGER.info("requestStatusBoard  gc {}  cleanCount {}", n1, cleanCount);
        }
    }

    public static class Stats {
        public long totalBytesEmitted;
        public long indexSizeSmall;
        public long indexSizeMedium;
        public long indexSizeLarge;
        public long indexSizeHuge;
        public Stats() {
            totalBytesEmitted = QueryData.totalBytesEmitted.get();
            indexSizeSmall = QueryData.indexSizeSmall.get();
            indexSizeMedium = QueryData.indexSizeMedium.get();
            indexSizeLarge = QueryData.indexSizeLarge.get();
            indexSizeHuge = QueryData.indexSizeHuge.get();
        }
    }

    public RequestStatusBoard requestStatusBoard() {
        return requestStatusBoard;
    }

    public Mono<ResponseEntity<Flux<DataBuffer>>> queryLocal(ReqCtx reqCtx, ServerWebExchange exchange, @RequestBody Mono<Query> queryMono) {
        ServerHttpRequest req = exchange.getRequest();
        Mono<Flux<DataBuffer>> mret = queryMono
        .doOnError(x -> LOGGER.info("can not parse request"))
        .map(query -> {
            QueryParams qp = QueryParams.fromQuery(query, reqCtx.bufCtx.bufFac, reqCtx.bufCtx.bufferSize);
            LOGGER.info(String.format("queryLocal  %s  %s  %s  %s", req.getId(), qp.begin, qp.end, qp.channels));
            long endNanos = 1000000L * qp.end.toEpochMilli();
            class MakeTrans implements MapFunctionFactory<EventBlobMapResult> {
                final ReqCtx reqctx;
                final QueryParams qp;
                final long endNanos;
                public MakeTrans(ReqCtx reqctx, QueryParams qp, long endNanos) {
                    this.reqctx = reqctx;
                    this.qp = qp;
                    this.endNanos = endNanos;
                }
                @Override
                public Flux<EventBlobMapResult> makeTrans(Flux<BufCont> fl, KeyspaceToDataParams kspp, int fileno) {
                    return EventBlobToV1Map.trans(reqctx, fl, kspp.ksp.channel.name, endNanos, kspp.bufFac, kspp.bufferSize, qp);
                }
            }
            Function<KeyspaceToDataParams, Mono<List<Flux<EventBlobMapResult>>>> keyspaceToData = p -> {
                return ChannelEventStream.dataFluxFromFiles(p, new MakeTrans(reqCtx, qp, endNanos), reqCtx.bufCtx);
            };
            return channelsToData(reqCtx, baseDirFinder, qp.channels, qp.begin, qp.end, qp.splits, reqCtx.bufCtx, keyspaceToData)
            .transform(doDiscard("queryLocalEnd"))
            .concatMapIterable(EventBlobMapResult::takeBufCont, 4)
            .map(BufCont::takeBuf)
            .filter(Optional::isPresent)
            .map(Optional::get);
        });
        return logResponse(reqCtx, "queryLocal", mret);
    }


    public Mono<ResponseEntity<Flux<DataBuffer>>> rawLocal(ReqCtx reqCtx, ServerWebExchange exchange, Mono<Query> queryMono) {
        String mainReqId2;
        try {
            mainReqId2 = exchange.getRequest().getHeaders().get("x-main-req-id").get(0);
        }
        catch (NullPointerException | IndexOutOfBoundsException e) {
            mainReqId2 = "NOREQID";
        }
        String mainReqId = mainReqId2;
        return queryMono
        .flatMap(query -> {
            return rawLocalInner(reqCtx, Mono.just(query), mainReqId)
            .flatMap(fl -> {
                return logResponse(reqCtx, "rawLocal", Mono.just(fl));
            });
        });
    }

    public Mono<Flux<DataBuffer>> rawLocalInner(ReqCtx reqCtx, Mono<Query> queryMono, String mainReqId) {
        requestStatusBoard.requestSubBegin(reqCtx, mainReqId);
        DataBufferFactory bufFac = reqCtx.bufCtx.bufFac;
        AtomicLong totalBytesEmit = new AtomicLong();
        Mono<Flux<DataBuffer>> mret = queryMono
        .doOnError(x -> LOGGER.error("{}  can not parse request", reqCtx))
        .map(query -> {
            QueryParams qp = QueryParams.fromQuery(query, reqCtx.bufCtx.bufFac, reqCtx.bufCtx.bufferSize);
            if (qp.channels.size() != 1) {
                LOGGER.error("{}  rawLocal  requested more than one channel  {}  {}  {}", reqCtx, qp.begin, qp.end, qp.channels);
                throw new RuntimeException("logic");
            }
            final String channelName = "" + qp.channels.get(0);
            LOGGER.info("{}  rawLocal  {}  {}  {}", reqCtx, qp.begin, qp.end, qp.channels);
            long endNanos = 1000000L * qp.end.toEpochMilli();
            Function<KeyspaceToDataParams, Mono<List<Flux<MapTsItemVec>>>> keyspaceToData = p -> {
                return ChannelEventStream.dataFluxFromFiles(p, new TransMapTsForRaw(reqCtx, qp, endNanos, channelName), reqCtx.bufCtx);
            };
            return channelsToData(reqCtx, baseDirFinder, qp.channels, qp.begin, qp.end, qp.splits, reqCtx.bufCtx, keyspaceToData)
            .doOnNext(item -> {
                if (!item.notTerm()) {
                    LOGGER.debug("{}  rawLocal  term item  channel {}", reqCtx, channelName);
                    item.release();
                }
            })
            .takeWhile(MapTsItemVec::notTerm)
            .doOnNext(kk -> kk.markWith(BufCont.Mark.QUERY_DATA_RAW_LOCAL_01))
            .concatMapIterable(MapTsItemVec::takeBuffers, 1)
            .doOnNext(kk -> kk.appendMark(BufCont.Mark.QUERY_DATA_RAW_LOCAL_02))
            .transform(doDiscard("rawLocalMiddle"))
            .map(BufCont::takeBuf)
            .filter(Optional::isPresent).map(Optional::get)
            .takeWhile(buf -> qp.limitBytesPerChannel == 0 || totalBytesEmit.get() < qp.limitBytesPerChannel)
            .doOnSubscribe(s -> {
                LOGGER.debug("{}  rawLocal sig  SUBSCRIBE {}", reqCtx, channelName);
            })
            .doOnCancel(() -> {
                LOGGER.info("{}  rawLocal sig  CANCEL    {}", reqCtx, channelName);
            })
            .doOnComplete(() -> {
                LOGGER.debug("{}  rawLocal sig  COMPLETE   {}  bytes {}", reqCtx, channelName, totalBytesEmit.get());
            })
            .doOnError(e -> {
                LOGGER.error("{}  rawLocal  ERROR  {}  {}", reqCtx, channelName, e.toString());
                requestStatusBoard.requestErrorChannelName(reqCtx, channelName, e);
            })
            .doOnNext(buf -> {
                int rb = buf.readableByteCount();
                totalBytesEmit.getAndAdd(rb);
                QueryData.totalBytesEmitted.getAndAdd(rb);
                reqCtx.addBodyLen(rb);
            })
            .filter(buf -> {
                if (buf.readableByteCount() > 0) {
                    return true;
                }
                else {
                    nFilteredEmptyRawOut.getAndAdd(1);
                    return false;
                }
            })
            .transform(fl -> Throttle.throttle(fl, qp.throttleRate / 8, qp.throttleSteps, qp.throttleInterval, qp.throttleOverslack))
            .transform(statusPing(reqCtx, this))
            .transform(doDiscard("rawLocalEnd"))
            .doFinally(sig -> requestStatusBoard.bodyEmitted(reqCtx, sig));
        })
        .map(fl -> {
            return Flux.just(bufFac.wrap(new byte[] { 'P', 'R', 'E', '0' }))
            .concatWith(fl);
        })
        .doOnError(e -> {
            LOGGER.error("{}  rawLocal  {}", reqCtx, e.toString());
            requestStatusBoard.requestError(reqCtx, e);
        });
        return mret;
    }


    interface MergeFunction {
        Flux<BufCont> apply(ReqCtx reqctx, List<Flux<MapTsItemVec>> lfl, String channelName, QueryParams qp, BufCtx bufCtx);
    }

    static class MergeFunctionDefault implements MergeFunction {
        @Override
        public Flux<BufCont> apply(ReqCtx reqctx, List<Flux<MapTsItemVec>> lfl, String channelName, QueryParams qp, BufCtx bufCtx) {
            return MergerSupport.flattenSlices(MergerSupport.mergeItemVecFluxes(lfl, qp), channelName, qp, bufCtx);
        }
    }

    static class MergeFunctionFake implements MergeFunction {
        @Override
        public Flux<BufCont> apply(ReqCtx reqctx, List<Flux<MapTsItemVec>> lfl, String channelName, QueryParams qp, BufCtx bufCtx) {
            //Flux<MapTsItemVec> all = lfl.get(0).subscribeOn(Schedulers.parallel());
            List<Flux<MapTsItemVec>> lfl2 = lfl.stream().map(k -> k.subscribeOn(Schedulers.parallel())).collect(Collectors.toList());
            return Flux.merge(Flux.fromIterable(lfl2), lfl.size(), 1)
            .concatMapIterable(k -> {
                return k.testTakeBuffers();
            });
            //return MergerSupport.flattenSlices(MergerSupport.mergeItemVecFluxes(lfl), channelName, qp, bufCtx);
        }
    }

    Flux<BufCont> pipeThroughMergerFake(ReqCtx reqctx, List<Flux<MapTsItemVec>> lfl, String channelName, QueryParams qp, BufCtx bufCtx) {
        return Flux.concat(lfl)
        .map(k -> {
            k.release();
            return BufCont.makeEmpty(BufCont.Mark.MERGER_FAKE);
        });
    }

    Flux<BufCont> pipeThroughMerger(ReqCtx reqctx, List<Flux<MapTsItemVec>> lfl, String channelName, QueryParams qp, BufCtx bufCtx) {
        return MergerSupport.flattenSlices(MergerSupport.mergeItemVecFluxes(lfl, qp), channelName, qp, bufCtx);
    }

    public Mono<ResponseEntity<Flux<DataBuffer>>> queryMergedOctets(ReqCtx reqCtx, Mono<Query> queryMono) {
        LOGGER.info("{}  queryMergedOctets", reqCtx);
        requestStatusBoard.requestBegin(reqCtx);
        AtomicLong haveNettyCount = new AtomicLong();
        AtomicLong nonNettyCount = new AtomicLong();
        Mono<Flux<DataBuffer>> mret = queryMono
        .doOnError(x -> LOGGER.error("{}  can not parse request", reqCtx))
        .map(query -> {
            QueryParams qp = QueryParams.fromQuery(query, reqCtx.bufCtx.bufFac, reqCtx.bufCtx.bufferSize);
            LOGGER.info("{}  queryMergedOctets  {}  {}  {}  {}", reqCtx, qp.begin, qp.end, qp.channels, qp.splits);
            AtomicLong nbytes = new AtomicLong();
            TransformSupplier<BufCont> valuemapSup;
            if (qp.valuemapType == 1) {
                valuemapSup = new TransMapQueryMergedFake(reqCtx, qp);
            }
            else {
                valuemapSup = new TransMapQueryMergedDefault(reqCtx, qp);
            }
            MergeFunction mergeFunction;
            if (qp.mergeType == 1) {
                mergeFunction = new MergeFunctionFake();
            }
            else {
                mergeFunction = new MergeFunctionDefault();
            }
            return SubTools.buildMerged(reqCtx, qp, valuemapSup, mergeFunction, requestStatusBoard(), conf)
            .doOnSubscribe(nreq1 -> {
                LOGGER.trace("{}  doOnSubscribe  queryMergedOctets: subscribing to result from buildMerged", reqCtx);
            })
            .doOnNext(bufcont -> {
                if (bufcont == null) {
                    LOGGER.error("{}  bufcont is null, can not add readable byte count", reqCtx);
                    throw new RuntimeException("logic");
                }
                LOGGER.trace(logMarkerQueryMergedItems, "{}  queryMerged  net emit  len {}", reqCtx, bufcont.readableByteCount());
                long h = nbytes.addAndGet(bufcont.readableByteCount());
                if (query.errorAfterBytes > 0 && h > query.errorAfterBytes) {
                    throw new RuntimeException("Byte limit reached");
                }
            })
            .doOnError(e -> {
                LOGGER.error("{}  queryMerged ERROR  {}", reqCtx, e.toString());
                requestStatusBoard.getOrCreate(reqCtx).addError(new RequestStatus.Error(e.toString()));
            })
            .map(BufCont::takeBuf)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .doOnNext(k -> {
                int rb = k.readableByteCount();
                QueryData.totalBytesEmitted.getAndAdd(rb);
                reqCtx.addBodyLen(rb);
            })
            .doOnNext(k -> {
                if (k instanceof NettyDataBuffer) {
                    haveNettyCount.getAndAdd(1);
                    ByteBuf bb = ((NettyDataBuffer) k).getNativeBuffer();
                    if (bb != null) {
                        if (bb.refCnt() == 0) {
                            LOGGER.info("bad output of refCnt 0");
                        }
                    }
                }
                else {
                    nonNettyCount.getAndAdd(1);
                }
            })
            .transform(fl -> Throttle.throttle(fl, qp.throttleRate, qp.throttleSteps, qp.throttleInterval, qp.throttleOverslack))
            .transform(statusPing(reqCtx, this))
            .transform(doDiscard("queryMergedOctetsFinal"))
            .doOnDiscard(Object.class, QueryData::doDiscardFinal)
            .doFinally(sig -> requestStatusBoard.bodyEmitted(reqCtx, sig))
            .doFinally(k -> {
                LOGGER.info("haveNettyCount {}  nonNettyCount {}", haveNettyCount.get(), nonNettyCount.get());
            });
        });
        return logResponse(reqCtx, "queryMergedOctets", mret);
    }

    public Mono<ResponseEntity<Flux<DataBuffer>>> queryMergedOctetsLocal(ReqCtx reqCtx, Mono<Query> queryMono) {
        return queryMono.map(q -> {
            return queryMergedOctetsLocal_current(reqCtx, Mono.just(q));
        })
        .flatMap(k -> k);
    }

    public Mono<ResponseEntity<Flux<DataBuffer>>> queryMergedOctetsLocal_current(ReqCtx reqCtx, Mono<Query> queryMono) {
        LOGGER.info("{}  queryMergedOctetsLocal", reqCtx);
        requestStatusBoard.requestBegin(reqCtx);
        Mono<Flux<DataBuffer>> mret = queryMono
        .doOnError(x -> LOGGER.error("{}  can not parse request", reqCtx))
        .map(query -> {
            QueryParams qp = QueryParams.fromQuery(query, reqCtx.bufCtx.bufFac, reqCtx.bufCtx.bufferSize);
            LOGGER.info("{}  queryMergedLocal  {}  {}  {}  {}", reqCtx, qp.begin, qp.end, qp.channels, qp.splits);
            Instant begin = Instant.parse(query.range.startDate);
            Instant end = Instant.parse(query.range.endDate);
            long endNanos = 1000000L * end.toEpochMilli();
            return Flux.fromIterable(query.channels)
            .concatMap(channelName -> {
                return Flux.fromIterable(conf.splits)
                .filter(split -> qp.splits == null || qp.splits.isEmpty() || qp.splits.contains(split))
                .<Flux<MapTsItemVec>>map(split -> {
                    LOGGER.info("{}  local {}  split {}  {}", reqCtx, conf.canonicalHostname, split, channelName);
                    Function<KeyspaceToDataParams, Mono<List<Flux<BufCont>>>> keyspaceToData = p -> {
                        return ChannelEventStream.dataFluxFromFiles(p, new MakeTrans2(), reqCtx.bufCtx);
                    };
                    return channelsToData(reqCtx, baseDirFinder, List.of(channelName), qp.begin, qp.end, List.of(split), reqCtx.bufCtx, keyspaceToData)
                    .transform(fl -> EventBlobToV1MapTs.trans(reqCtx, fl, String.format("__sn%02d__QML", split), channelName, endNanos, reqCtx.bufCtx));
                })
                .collectList()
                .<BufCont>flatMapMany(lfl -> pipeThroughMerger(reqCtx, lfl, channelName, qp, reqCtx.bufCtx))
                .transform(flbuf -> EventBlobToV1Map.trans(reqCtx, flbuf, channelName, endNanos, qp.bufFac, qp.bufferSize, qp))
                .concatMapIterable(EventBlobMapResult::takeBufCont, 4)
                .transform(doDiscard("queryMergedOctetsLocalChannelEnd"));
            }, 1)
            .map(BufCont::takeBuf)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .doOnNext(k -> {
                int rb = k.readableByteCount();
                QueryData.totalBytesEmitted.getAndAdd(rb);
                reqCtx.addBodyLen(rb);
            })
            .transform(fl -> Throttle.throttle(fl, qp.throttleRate, qp.throttleSteps, qp.throttleInterval, qp.throttleOverslack))
            .transform(statusPing(reqCtx, this))
            .transform(doDiscard("queryMergedOctetsLocalEnd"))
            .doFinally(sig -> requestStatusBoard.bodyEmitted(reqCtx, sig));
        });
        return logResponse(reqCtx, "queryMergedOctetsLocal", mret);
    }


    public Mono<ResponseEntity<Flux<DataBuffer>>> queryMergedJson(ReqCtx reqCtx, ServerWebExchange exchange, @RequestBody Mono<Query> queryMono) {
        requestStatusBoard.requestBegin(reqCtx);
        ServerHttpRequest req = exchange.getRequest();
        if (!req.getHeaders().getAccept().contains(MediaType.APPLICATION_JSON)) {
            LOGGER.warn("{}  queryMerged  Client omits Accept: {}", req.getId(), MediaType.APPLICATION_JSON);
        }
        Mono<Flux<DataBuffer>> mret = queryMono
        .doOnError(x -> LOGGER.info("can not parse request"))
        .map(query -> {
            BinFind binFind;
            List<AggFunc> aggs;
            if (query.aggregation != null) {
                binFind = new BinFind(query.aggregation.nrOfBins, query.range);
                aggs = new ArrayList<>();
                for (String n : query.aggregation.aggregations) {
                    if (n.equals("sum")) {
                        aggs.add(new AggSum());
                    }
                    else if (n.equals("mean")) {
                        aggs.add(new AggMean());
                    }
                    else if (n.equals("min")) {
                        aggs.add(new AggMin());
                    }
                    else if (n.equals("max")) {
                        aggs.add(new AggMax());
                    }
                }
            }
            else {
                binFind = null;
                aggs = null;
            }
            QueryParams qp = QueryParams.fromQuery(query, reqCtx.bufCtx.bufFac, reqCtx.bufCtx.bufferSize);
            LOGGER.info(String.format("%s  queryMerged  %s  %s  %s", req.getId(), qp.begin, qp.end, qp.channels));
            AggMapper mapper = new AggMapper(binFind, aggs, qp.bufFac);
            return SubTools.buildMerged(reqCtx, qp, new TransformSup3(qp), new MergeFunctionDefault(), requestStatusBoard(), conf)
            .map(mapper::map)
            .concatWith(Mono.defer(() -> Mono.just(mapper.finalResult())))
            .concatMapIterable(Function.identity(), 1)
            .doFinally(k -> mapper.release())
            .doOnNext(k -> {
                int rb = k.readableByteCount();
                QueryData.totalBytesEmitted.getAndAdd(rb);
                reqCtx.addBodyLen(rb);
            })
            .transform(fl -> Throttle.throttle(fl, qp.throttleRate, qp.throttleSteps, qp.throttleInterval, qp.throttleOverslack))
            .transform(statusPing(reqCtx, this))
            .doFinally(sig -> requestStatusBoard.bodyEmitted(reqCtx, sig));
        });
        return mret.map(x -> {
            return ResponseEntity.ok()
            .header(HEADER_X_CANONICALHOSTNAME, conf.canonicalHostname)
            .header(HEADER_X_DAQBUFFER_REQUEST_ID, reqCtx.reqId)
            .contentType(MediaType.APPLICATION_JSON)
            .body(x);
        });
    }

    <T> Mono<ResponseEntity<Flux<T>>> logResponse(ReqCtx reqCtx, String name, Mono<Flux<T>> m) {
        return m.map(fl -> {
            return ResponseEntity.ok()
            .header(HEADER_X_CANONICALHOSTNAME, conf.canonicalHostname)
            .header(HEADER_X_DAQBUFFER_REQUEST_ID, reqCtx.reqId)
            .contentType(MediaType.APPLICATION_OCTET_STREAM)
            .body(fl);
        });
    }

    public static <T> Function<Flux<T>, Flux<T>> doDiscard(String id) {
        return fl -> fl.doOnDiscard(Object.class, obj -> doDiscard(id, obj));
    }

    public static <T> Function<Mono<T>, Mono<T>> doDiscardMono(String id) {
        return fl -> fl.doOnDiscard(Object.class, obj -> doDiscard(id, obj));
    }

    public static void doDiscard(String id, Object obj) {
        if (obj instanceof BufCont) {
            // TODO count these cases
            //LOGGER.info("doDiscard  {}  BufCont", id);
            ((BufCont) obj).close();
        }
        else if (obj instanceof MapTsItemVec) {
            //LOGGER.info("doDiscard  {}  MapTsItemVec", id);
            ((MapTsItemVec) obj).release();
        }
        else if (obj instanceof MapTsToken) {
            //LOGGER.info("doDiscard  {}  MapTsToken", id);
            ((MapTsToken) obj).release();
        }
        else if (obj instanceof EventBlobMapResult) {
            //LOGGER.info("doDiscard  {}  EventBlobMapResult", id);
            ((EventBlobMapResult) obj).release();
        }
        else if (obj instanceof PositionedDatafile) {
            try {
                ((PositionedDatafile) obj).release();
            }
            catch (Throwable e) {
                LOGGER.warn("exception while discard {}", e.toString());
            }
        }
        else if (obj instanceof Releasable) {
            //LOGGER.info("doDiscard  {}  Releasable  {}", id, obj.getClass().getSimpleName());
            ((Releasable) obj).releaseFinite();
        }
        else if (obj instanceof Optional) {
            Optional opt = (Optional) obj;
            if (opt.isPresent()) {
                String sn = opt.get().getClass().getSimpleName();
                if (sn.equals("MonoOnAssembly")) {
                }
                else {
                    LOGGER.info("doDiscard  {}  Optional  {}", id, sn);
                }
            }
        }
        else if (obj instanceof NettyDataBuffer) {
        }
        else if (obj instanceof LimitedDataBufferList) {
        }
        else if (obj instanceof Integer) {
        }
        else if (obj instanceof String) {
        }
        else if (obj instanceof TimeBin2) {
        }
        else if (obj != null) {
            LOGGER.error("doDiscard  {}  UNKNOWN  {}", id, obj.getClass().getSimpleName());
        }
        else {
            LOGGER.error("doDiscard  {}  null", id);
        }
    }

    public static void doDiscardFinal(Object obj) {
        if (obj instanceof DataBuffer) {
            //LOGGER.info("doDiscard  {}  DataBuffer", id);
            DataBufferUtils.release((DataBuffer) obj);
        }
        else if (obj instanceof LimitedDataBufferList) {
            LOGGER.info("doDiscard  LimitedDataBufferList");
            ((LimitedDataBufferList) obj).releaseAndClear();
        }
    }

    static class StatusPinger {
        long tsLast;
        StatusPinger(long tsLast) {
            this.tsLast = tsLast;
        }
    }

    static <T> Function<Flux<T>, Flux<T>> statusPing(ReqCtx reqCtx, QueryData queryData) {
        StatusPinger p = new StatusPinger(System.nanoTime());
        return fl -> fl.doOnNext(_x -> {
            long ts = System.nanoTime();
            if (ts - p.tsLast > 1000L * 1000 * 1000 * 5) {
                p.tsLast = ts;
                queryData.requestStatusBoard.ping(reqCtx);
            }
        });
    }

    static Scheduler fs = Schedulers.newParallel("fs", 16);

    <T extends Releasable> Flux<T> channelsToData(ReqCtx reqCtx, BaseDirFinderFormatV0 baseDirFinder, List<String> channels, Instant begin, Instant end, List<Integer> splitsIn, BufCtx bufCtx, Function<KeyspaceToDataParams, Mono<List<Flux<T>>>> keyspaceToData) {
        if (splitsIn == null) {
            splitsIn = List.of();
        }
        List<Integer> splits = splitsIn;
        return Flux.fromIterable(channels)
        .subscribeOn(fs)
        .concatMap(channelName -> {
            return baseDirFinder.findMatchingDataFiles(reqCtx, channelName, begin, end, splits, bufCtx.bufFac)
            .doOnNext(x -> {
                if (x.keyspaces.size() < 1) {
                    LOGGER.warn(String.format("no keyspace found for channel %s", channelName));
                }
                else if (x.keyspaces.size() > 1) {
                    LOGGER.warn(String.format("more than one keyspace for %s", channelName));
                }
                else {
                    if (false) {
                        LOGGER.info("Channel {} using files: {}", channelName, x.keyspaces.get(0).splits.stream().map(sp -> sp.timeBins.size()).reduce(0, Integer::sum));
                    }
                }
            })
            .flatMapIterable(x2 -> x2.keyspaces)
            .concatMap(ks -> {
                KeyspaceToDataParams p = new KeyspaceToDataParams(reqCtx, ks, begin, end, bufCtx.bufFac, bufCtx.bufferSize, splits);
                return keyspaceToData.apply(p);
            }, 0)
            .concatMap(x -> {
                if (x.size() <= 0) {
                    throw new RuntimeException("logic");
                }
                if (x.size() > 1) {
                    LOGGER.error("x.size() > 1   {}", x.size());
                    throw new RuntimeException("not yet supported in local query");
                }
                return x.get(0);
            }, 0)
            .transform(doDiscard("channelsToDataA"));
        }, 0)
        .transform(doDiscard("channelsToDataB"));
    }

}
