package ch.psi.daq.retrieval.controller;

import ch.psi.daq.retrieval.config.Node;
import ch.psi.daq.retrieval.controller.octets.MergedOctetsDistri;
import ch.psi.daq.retrieval.finder.MatchingChannelConfigRange;
import ch.psi.daq.retrieval.pod.api1.Channel;
import ch.psi.daq.retrieval.status.RequestStatusResult;
import ch.psi.daq.retrieval.utils.ChannelEventStream;
import ch.psi.daq.retrieval.utils.PositionedDatafile;
import ch.psi.daq.retrieval.QueryParams;
import ch.psi.daq.retrieval.reqctx.ReqCtx;
import ch.psi.daq.retrieval.bytes.BufCont;
import ch.psi.daq.retrieval.config.ConfigurationRetrieval;
import ch.psi.daq.retrieval.controller.json.MergeJson;
import ch.psi.daq.retrieval.controller.octets.MergedOctetsLocal;
import ch.psi.daq.retrieval.controller.raw.RawLocal;
import ch.psi.daq.retrieval.eventmap.ts.MapTsItemVec;
import ch.psi.daq.retrieval.eventmap.ts.MapTsToken;
import ch.psi.daq.retrieval.eventmap.value.EventBlobMapResult;
import ch.psi.daq.retrieval.eventmap.value.EventBlobToV1Map;
import ch.psi.daq.retrieval.finder.BaseDirFinderFormatV0;
import ch.psi.daq.retrieval.finder.TimeBin2;
import ch.psi.daq.retrieval.merger.Releasable;
import ch.psi.daq.retrieval.pod.api1.Query;
import ch.psi.daq.retrieval.status.RequestStatusBoard;
import ch.qos.logback.classic.Logger;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledDirectByteBuf;
import io.netty.buffer.UnpooledHeapByteBuf;
import io.netty.buffer.UnpooledUnsafeDirectByteBuf;
import io.netty.buffer.UnpooledUnsafeHeapByteBuf;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.core.io.buffer.LimitedDataBufferList;
import org.springframework.core.io.buffer.NettyDataBuffer;
import org.springframework.core.io.buffer.PooledDataBuffer;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

public class QueryData {
    static Logger LOGGER = (Logger) LoggerFactory.getLogger(QueryData.class.getSimpleName());
    public BaseDirFinderFormatV0 baseDirFinder;
    public ConfigurationRetrieval conf;
    public static final String HEADER_X_DAQBUFFER_REQUEST_ID = "x-daqbuffer-request-id";
    public static final String HEADER_X_CANONICALHOSTNAME = "X-CanonicalHostname";
    public static AtomicLong totalBytesEmitted = new AtomicLong();
    public static AtomicLong rawLocalBytesEmitted = new AtomicLong();
    public static AtomicLong indexSizeSmall = new AtomicLong();
    public static AtomicLong indexSizeMedium = new AtomicLong();
    public static AtomicLong indexSizeLarge = new AtomicLong();
    public static AtomicLong indexSizeHuge = new AtomicLong();
    public static AtomicLong rawLocalClosedBufCont = new AtomicLong();
    public static AtomicLong subTools1GapCount = new AtomicLong();
    public static AtomicLong discardReleasableListItem = new AtomicLong();
    public static AtomicLong discardReleasedMapTsTokenFromList = new AtomicLong();
    public RequestStatusBoard requestStatusBoard;
    public int port;

    static Marker logMarkerWebClientResponse = MarkerFactory.getMarker("WebClientResponse");
    static Marker logMarkerWebClientResponseItem = MarkerFactory.getMarker("WebClientResponseItem");
    static Marker logMarkerQueryMergedItems = MarkerFactory.getMarker("QueryMergedItems");
    static Marker logMarkerRawLocalItem = MarkerFactory.getMarker("RawLocalItem");
    static Marker logMarkerAccept = MarkerFactory.getMarker("Accept");

    static {
        LOGGER.getLoggerContext().addTurboFilter(new ItemFilter());
    }

    public static Scheduler clientsplitnodeiter = Schedulers.newBoundedElastic(128, 512, "csni");

    public QueryData(BaseDirFinderFormatV0 finder, ConfigurationRetrieval conf) {
        this.baseDirFinder = finder;
        this.conf = conf;
        requestStatusBoard = new RequestStatusBoard(conf);
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
            LOGGER.debug("requestStatusBoard  gc {}  cleanCount {}", n1, cleanCount);
        }
    }

    public static class Stats {
        public long totalBytesEmitted;
        public long rawLocalBytesEmitted;
        public long indexSizeSmall;
        public long indexSizeMedium;
        public long indexSizeLarge;
        public long indexSizeHuge;
        public long rawLocalClosedBufCont;
        public long subTools1GapCount;
        public long discardReleasableListItem;
        public long discardReleasedMapTsTokenFromList;
        public Stats() {
            totalBytesEmitted = QueryData.totalBytesEmitted.get();
            rawLocalBytesEmitted = QueryData.rawLocalBytesEmitted.get();
            indexSizeSmall = QueryData.indexSizeSmall.get();
            indexSizeMedium = QueryData.indexSizeMedium.get();
            indexSizeLarge = QueryData.indexSizeLarge.get();
            indexSizeHuge = QueryData.indexSizeHuge.get();
            rawLocalClosedBufCont = QueryData.rawLocalClosedBufCont.get();
            subTools1GapCount = QueryData.subTools1GapCount.get();
            discardReleasableListItem = QueryData.discardReleasableListItem.get();
            discardReleasedMapTsTokenFromList = QueryData.discardReleasedMapTsTokenFromList.get();
        }
    }

    public RequestStatusBoard requestStatusBoard() {
        return requestStatusBoard;
    }

    public Mono<ResponseEntity<Flux<DataBuffer>>> queryNoMerge(ReqCtx reqCtx, ServerWebExchange exchange, @RequestBody Mono<Query> queryMono) {
        ServerHttpRequest req = exchange.getRequest();
        Mono<Flux<DataBuffer>> mret = queryMono
        .doOnError(x -> LOGGER.info("can not parse request"))
        .map(query -> {
            QueryParams qp = QueryParams.fromQuery(conf, query, reqCtx.bufCtx);
            LOGGER.info(String.format("queryLocal  %s  %s  %s  %s", req.getId(), qp.begin, qp.end, qp.channels));
            long endNanos = 1000000L * qp.end.toEpochMilli();
            List<Integer> splits = conf.splits.stream()
            .filter(split -> qp.splits == null || qp.splits.isEmpty() || qp.splits.contains(split))
            .collect(Collectors.toList());
            if (splits.size() != 1) {
                LOGGER.error("{}  queryNoMerge  can only handle a single split  splits {}", reqCtx, splits);
                throw new RuntimeException("logic");
            }
            int split = splits.get(0);
            return Flux.fromIterable(qp.channels)
            .concatMap(channel -> {
                return ChannelEventStream.channelDataFluxes(reqCtx, qp, baseDirFinder, qp.begin, qp.end, Duration.ZERO, channel.name, split)
                .flatMapMany(res -> EventBlobToV1Map.trans(reqCtx, res.fl, channel.name, endNanos, reqCtx.bufCtx, qp))
                .transform(doDiscard("queryLocalEnd"))
                .concatMapIterable(EventBlobMapResult::takeBufCont, 4)
                .map(BufCont::takeBuf)
                .filter(Optional::isPresent)
                .map(Optional::get);
            });
        });
        return logResponse(reqCtx, "queryLocal", mret);
    }

    public Mono<ResponseEntity<Flux<DataBuffer>>> queryMergedOctetsLocal(ReqCtx reqCtx, Mono<Query> queryMono) {
        return new MergedOctetsLocal().queryMergedOctetsLocal(reqCtx, this, queryMono);
    }

    public Mono<ResponseEntity<Flux<DataBuffer>>> rawLocal(ReqCtx reqCtx, ServerWebExchange exchange, Mono<Query> queryMono) {
        return new RawLocal().rawLocal(reqCtx, this, exchange, queryMono);
    }

    public Mono<ResponseEntity<Flux<DataBuffer>>> queryMergedOctets(ReqCtx reqCtx, Mono<Query> queryMono) {
        return new MergedOctetsDistri().queryMergedOctets(reqCtx, this, queryMono);
    }

    public Mono<ResponseEntity<Flux<DataBuffer>>> queryMergedJson(ReqCtx reqCtx, Mono<Query> queryMono) {
        return new MergeJson().queryMergedJson(reqCtx, this, queryMono);
    }

    public <T> Mono<ResponseEntity<Flux<T>>> logResponse(ReqCtx reqCtx, String name, Mono<Flux<T>> m) {
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
        else if (obj instanceof RequestStatusResult) {
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
        else if (obj instanceof LimitedDataBufferList) {
            LimitedDataBufferList l = (LimitedDataBufferList) obj;
            for (DataBuffer buf : l) {
                DataBufferUtils.release(buf);
            }
        }
        else if (obj instanceof List) {
            List al = (List) obj;
            if (al.size() > 0) {
                Object o = al.get(0);
                if (o instanceof Releasable) {
                    for (Object j : al) {
                        Releasable q = (Releasable) j;
                        q.releaseFinite();
                    }
                }
                else {
                    LOGGER.error("doDiscard  {}  List of {}", id, o.getClass().getSimpleName());
                }
            }
            else {
                LOGGER.error("doDiscard  {}  List of unknown element type", id);
            }
        }
        else if (obj instanceof PooledDataBuffer) {
        }
        else if (obj instanceof ByteBuf) {
            ByteBuf buf = (ByteBuf) obj;
            buf.release();
        }
        else if (obj instanceof UnpooledUnsafeHeapByteBuf) {
        }
        else if (obj instanceof UnpooledHeapByteBuf) {
        }
        else if (obj instanceof UnpooledUnsafeDirectByteBuf) {
        }
        else if (obj instanceof UnpooledDirectByteBuf) {
        }
        else if (obj instanceof MatchingChannelConfigRange) {
        }
        else if (obj instanceof Integer) {
        }
        else if (obj instanceof String) {
        }
        else if (obj instanceof TimeBin2) {
        }
        else if (obj instanceof Node) {
        }
        else if (obj instanceof Channel) {
        }
        else if (obj != null) {
            String cn = obj.getClass().getSimpleName();
            if (cn.equals("PooledUnsafeDirectByteBuf")) {
            }
            else {
                LOGGER.error("doDiscard ...  {}  UNKNOWN  {}", id, cn);
            }
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

    public <T> Function<Flux<T>, Flux<T>> statusPing(ReqCtx reqCtx) {
        StatusPinger p = new StatusPinger(System.nanoTime());
        return fl -> fl.doOnNext(_x -> {
            long ts = System.nanoTime();
            if (ts - p.tsLast > 1000L * 1000 * 1000 * 5) {
                p.tsLast = ts;
                requestStatusBoard.ping(reqCtx);
            }
        });
    }

}
