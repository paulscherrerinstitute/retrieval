package ch.psi.daq.retrieval.controller;

import ch.psi.daq.retrieval.*;
import ch.psi.daq.retrieval.eventmap.ts.EventBlobToV1MapTs;
import ch.psi.daq.retrieval.eventmap.ts.Item;
import ch.psi.daq.retrieval.eventmap.value.*;
import ch.psi.daq.retrieval.finder.BaseDirFinderFormatV0;
import ch.psi.daq.retrieval.merger.Merger;
import ch.psi.daq.retrieval.pod.api1.Query;
import ch.psi.daq.retrieval.pod.api1.Range;
import ch.psi.daq.retrieval.status.RequestStatus;
import ch.psi.daq.retrieval.status.RequestStatusBoard;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.turbo.TurboFilter;
import ch.qos.logback.core.spi.FilterReply;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuples;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class QueryData {
    static Logger LOGGER = (Logger) LoggerFactory.getLogger(QueryData.class);
    DataBufferFactory defaultDataBufferFactory = new DefaultDataBufferFactory();
    int bufferSize = 64 * 1024;
    public BaseDirFinderFormatV0 baseDirFinder;
    AtomicLong nFilteredEmptyRawOut = new AtomicLong();
    List<SplitNode> splitNodes;
    AtomicLong totalBytesServed = new AtomicLong();
    String canonicalHostname;
    final RequestStatusBoard requestStatusBoard = new RequestStatusBoard();

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

    static Scheduler clientsplitnodeiter = Schedulers.newParallel("csn", 16);

    public QueryData(BaseDirFinderFormatV0 finder, List<SplitNode> splitNodes, String canonicalHostname) {
        this.baseDirFinder = finder;
        this.splitNodes = splitNodes;
        this.canonicalHostname = canonicalHostname;
    }

    long cleanCount = 0;
    @Scheduled(fixedRate = 4000)
    void scheduledStatusClean() {
        cleanCount += 1;
        long n1 = requestStatusBoard.gc();
        if (n1 > 0) {
            cleanCount = 0;
            LOGGER.info("requestStatusBoard  gc {}  cleanCount {}", n1, cleanCount);
        }
    }

    public RequestStatusBoard requestStatusBoard() {
        return requestStatusBoard;
    }

    public Mono<ResponseEntity<Flux<DataBuffer>>> queryLocal(ReqCtx reqctx, ServerWebExchange exchange, @RequestBody Mono<Query> queryMono) {
        ServerHttpRequest req = exchange.getRequest();
        Mono<Flux<DataBuffer>> mret = queryMono
        .doOnError(x -> LOGGER.info("can not parse request"))
        .map(query -> {
            QueryParams qp = QueryParams.fromQuery(query, defaultDataBufferFactory, bufferSize);
            LOGGER.info(String.format("queryLocal  %s  %s  %s  %s", req.getId(), qp.begin, qp.end, qp.channels));
            long endNanos = 1000000L * qp.end.toEpochMilli();
            class MakeTrans implements MapFunctionFactory<EventBlobMapResult> {
                ReqCtx reqctx;
                QueryParams qp;
                long endNanos;
                public MakeTrans(ReqCtx reqctx, QueryParams qp, long endNanos) {
                    this.reqctx = reqctx;
                    this.qp = qp;
                    this.endNanos = endNanos;
                }
                @Override
                public Flux<EventBlobMapResult> makeTrans(Flux<DataBuffer> fl, KeyspaceToDataParams kspp, int fileno) {
                    return EventBlobToV1Map.trans2(reqctx, fl, kspp.ksp.channel.name, endNanos, kspp.bufFac, kspp.bufferSize, qp.decompressOnServer, qp.limitBytes);
                }
            }
            Function<KeyspaceToDataParams, Mono<List<Flux<EventBlobMapResult>>>> keyspaceToData = p -> {
                return ChannelEventStream.dataFluxFromFiles(p, new MakeTrans(reqctx, qp, endNanos));
            };
            return channelsToData(reqctx, exchange, baseDirFinder, qp.channels, qp.begin, qp.end, qp.splits, qp.bufFac, qp.bufferSize, keyspaceToData)
            .doOnDiscard(EventBlobMapResult.class, obj -> obj.release())
            .map(x -> x.buf);
        });
        return logResponse(reqctx, "queryLocal", mret);
    }

    static class TransMapTsForRaw implements MapFunctionFactory<Item> {
        ReqCtx reqctx;
        QueryParams qp;
        long endNanos;
        String channelName;
        public TransMapTsForRaw(ReqCtx reqctx, QueryParams qp, long endNanos, String channelName) {
            this.reqctx = reqctx;
            this.qp = qp;
            this.endNanos = endNanos;
            this.channelName = channelName;
        }
        @Override
        public Flux<Item> makeTrans(Flux<DataBuffer> fl, KeyspaceToDataParams kspp, int fileno) {
            return EventBlobToV1MapTs.trans2(reqctx, EventBlobToV1MapTs.Mock.NONE, fl, String.format("rawLocal_sp%02d/%d_f%02d", qp.splits.get(0), qp.splits.size(), fileno), channelName, endNanos, kspp.bufFac, qp.bufferSize);
        }
    }

    public Mono<ResponseEntity<Flux<DataBuffer>>> rawLocal(ReqCtx reqctx, ServerWebExchange exchange, @RequestBody Mono<Query> queryMono) {
        ServerHttpRequest req = exchange.getRequest();
        String mainReqId;
        try {
            mainReqId = req.getHeaders().get("x-main-req-id").get(0);
        }
        catch (NullPointerException | IndexOutOfBoundsException e) {
            mainReqId = "NOREQID";
        }
        requestStatusBoard.requestSubBegin(reqctx, mainReqId);
        DataBufferFactory bufFac = exchange.getResponse().bufferFactory();
        AtomicLong totalBytesEmit = new AtomicLong();
        Mono<Flux<DataBuffer>> mret = queryMono
        .doOnError(x -> LOGGER.error("{}  can not parse request", reqctx))
        .map(query -> {
            QueryParams qp = QueryParams.fromQuery(query, defaultDataBufferFactory, bufferSize);
            if (qp.channels.size() != 1) {
                LOGGER.error("{}  rawLocal  requested more than one channel  {}  {}  {}", reqctx, qp.begin, qp.end, qp.channels);
                throw new RuntimeException("logic");
            }
            final String channelName = "" + qp.channels.get(0);
            LOGGER.debug("{}  rawLocal  {}  {}  {}", reqctx, qp.begin, qp.end, qp.channels);
            long endNanos = 1000000L * qp.end.toEpochMilli();
            Function<KeyspaceToDataParams, Mono<List<Flux<Item>>>> keyspaceToData = p -> {
                return ChannelEventStream.dataFluxFromFiles(p, new TransMapTsForRaw(reqctx, qp, endNanos, channelName));
            };
            return channelsToData(reqctx, exchange, baseDirFinder, qp.channels, qp.begin, qp.end, qp.splits, qp.bufFac, qp.bufferSize, keyspaceToData)
            .doOnNext(item -> {
                if (item.isTerm()) {
                    LOGGER.debug("{}  rawLocal  term item  channel {}", reqctx, channelName);
                    item.release();
                }
            })
            .doOnDiscard(Item.class, item -> {
                LOGGER.warn("{}  rawLocal  discard item  channel {}", reqctx, channelName);
                item.release();
            })
            .takeWhile(item -> !item.isTerm())
            .flatMapIterable(item -> item.takeBuffers())
            .doOnNext(buf -> {
                reqctx.addBodyLen(buf);
                totalBytesEmit.getAndAdd(buf.readableByteCount());
            })
            .doOnSubscribe(s -> {
                LOGGER.debug("{}  rawLocal sig  SUBSCRIBE {}", reqctx, channelName);
            })
            .doOnCancel(() -> {
                LOGGER.info("{}  rawLocal sig  CANCEL    {}", reqctx, channelName);
            })
            .doOnComplete(() -> {
                LOGGER.debug("{}  rawLocal sig  COMPLETE   {}  bytes {}", reqctx, channelName, totalBytesEmit.get());
            })
            .doOnError(e -> {
                LOGGER.error("{}  rawLocal  ERROR  {}  {}", reqctx, channelName, e.toString());
                requestStatusBoard.requestErrorChannelName(reqctx, channelName, e);
            })
            .doOnTerminate(() -> {
                requestStatusBoard.bodyEmitted(reqctx);
                LOGGER.debug("{}  rawLocal sig  TERMINATE  {}", reqctx, channelName);
            })
            .filter(buf -> {
                if (buf.readableByteCount() > 0) {
                    return true;
                }
                else {
                    nFilteredEmptyRawOut.getAndAdd(1);
                    DataBufferUtils.release(buf);
                    return false;
                }
            });
        })
        .map(fl -> {
            return Flux.just(bufFac.wrap(new byte[] { 'P', 'R', 'E', '0' }))
            .concatWith(fl);
        })
        .doOnError(e -> {
            LOGGER.error("{}  rawLocal  {}", reqctx, e.toString());
            requestStatusBoard.requestError(reqctx, e);
        });
        return logResponse(reqctx, "rawLocal", mret);
    }


    static class TokenSinker {
        BlockingQueue<Integer> queue;
        AtomicLong n1 = new AtomicLong();
        TokenSinker(int n) {
            queue = new LinkedBlockingQueue<>(3 * n + 3);
            for (int i1 = 0; i1 < n; i1 += 1) {
                putBack(i1);
            }
        }
        void putBack(int token) {
            queue.add((((int)n1.getAndAdd(1)) * 10000) + (token % 10000));
        }
    }

    Mono<ClientResponse> springWebClientRequest(ReqCtx reqctx, String localURL, String channelName, String js) {
        return WebClient.builder()
        .baseUrl(localURL)
        .build()
        .post()
        .contentType(MediaType.APPLICATION_JSON)
        .accept(MediaType.APPLICATION_OCTET_STREAM)
        .header("Connection", "close")
        .header("x-main-req-id", reqctx.reqId)
        .body(BodyInserters.fromValue(js))
        .exchange()
        .doOnError(e -> {
            LOGGER.error("{}  WebClient exchange doOnError {}", reqctx, e.toString());
        })
        .doOnNext(x -> {
            String remote_reqid = x.headers().header("x-daqbuffer-request-id").get(0);
            LOGGER.debug("{}  WebClient got status {}  {}  remote x-daqbuffer-request-id {}", reqctx, x.statusCode(), channelName, remote_reqid);
            if (x.statusCode() != HttpStatus.OK) {
                LOGGER.error("{}  WebClient got status {}  {}", reqctx, x.statusCode(), channelName);
                throw new RuntimeException("sub request not OK");
            }
            if (!x.headers().header("connection").contains("close")) {
                LOGGER.error("{}  WebClient no conn close header  {}", reqctx, channelName);
                for (Map.Entry<String, List<String>> e : x.headers().asHttpHeaders().entrySet()) {
                    LOGGER.error("{}  header: {}", reqctx, e.getKey());
                    for (String v : e.getValue()) {
                        LOGGER.error("{}   v: {}", reqctx, v);
                    }
                }
                LOGGER.error("{}  {}", reqctx, x.headers().toString());
                throw new RuntimeException("Expect Connection close in answer");
            }
        });
    }

    enum RequestStatusResultType {
        RequestStaus,
        ByteBuffer,
    }

    static class RequestStatusResult {
        RequestStatusResultType ty;
        RequestStatus requestStatus;
        ByteBuffer byteBuffer;
        RequestStatusResult(RequestStatus k) {
            ty = RequestStatusResultType.RequestStaus;
            requestStatus = k;
        }
        RequestStatusResult(ByteBuffer k) {
            ty = RequestStatusResultType.ByteBuffer;
            byteBuffer = k;
        }
    }

    Mono<RequestStatusResult> getRequestStatus(ReqCtx reqctx, String host, int port, String reqId) {
        String requestStatusUrl = String.format("http://%s:%d/api/1/requestStatus/%s", host, port, reqId);
        return WebClient.builder()
        .baseUrl(requestStatusUrl)
        .build()
        .get()
        .exchange()
        .doOnError(e2 -> {
            LOGGER.error("{}  requestStatus exchange doOnError {}", reqctx, e2.toString());
        })
        .flatMap(x -> {
            if (x.statusCode() == HttpStatus.OK) {
                LOGGER.debug("{}  requestStatus  http status {}", reqctx, x.statusCode());
            }
            else {
                LOGGER.error("{}  requestStatus  http status {}", reqctx, x.statusCode());
            }
            return x.bodyToMono(ByteBuffer.class);
        })
        .map(buf -> {
            String remoteBodyMsg = StandardCharsets.UTF_8.decode(buf).toString();
            try {
                LOGGER.debug("{}  remote gave message {}", reqctx, remoteBodyMsg);
                return new RequestStatusResult((new ObjectMapper()).readValue(remoteBodyMsg, RequestStatus.class));
            }
            catch (IOException e2) {
                LOGGER.error("{}  getRequestStatus can not parse  {}  {}", reqctx, e2.toString(), remoteBodyMsg);
                return new RequestStatusResult(buf);
            }
        });
    }

    Flux<DataBuffer> pipeThroughMergerFake(ReqCtx reqctx, List<Flux<Item>> lfl, String channelName, QueryParams qp) {
        return Flux.concat(lfl)
        .map(item -> {
            if (item.item1 == null) {
                throw new RuntimeException("item1 is null");
            }
            DataBuffer ret = item.item1.buf;
            if (ret == null) {
                LOGGER.error("{}  null item", reqctx);
                throw new RuntimeException("bad null");
            }
            if (item.item2 != null && item.item2.buf != null) {
                DataBufferUtils.release(item.item2.buf);
            }
            return ret;
        });
    }

    Flux<DataBuffer> pipeThroughMerger(ReqCtx reqctx, List<Flux<Item>> lfl, String channelName, QueryParams qp) {
        AtomicLong totalSeenItems = new AtomicLong();
        AtomicLong totalRequestedItems = new AtomicLong();
        return Flux.from(new Merger(reqctx, channelName, lfl, qp.bufFac, qp.bufferSize))
        .doOnRequest(n -> {
            if (n > 50000) {
                LOGGER.warn("{}  large item request {}", reqctx, n);
            }
            long tot = totalRequestedItems.addAndGet(n);
            long rec = totalSeenItems.get();
            LOGGER.debug("{}  API_1_0_1 FL requesting from Merger  {}  total {}  seen {}", reqctx, n, tot, rec);
        })
        .doOnCancel(() -> {
            LOGGER.info("{}  API_1_0_1 FL cancel Merger", reqctx);
        })
        .doOnNext(buf -> {
            long rec = totalSeenItems.addAndGet(1);
            long tot = totalRequestedItems.get();
            LOGGER.trace("{}  API_1_0_1 FL item   total {}  seen {}", reqctx, tot, rec);
        })
        .doOnTerminate(() -> {
            LOGGER.debug("{}  API_1_0_1 FL TERMINATED", reqctx);
        });
    }

    <T> Flux<T> buildMerged(ReqCtx reqctx, QueryParams qp, TransformSupplier<T> transformSupplier) {
        final int nChannels = qp.channels.size();
        final AtomicLong oChannels = new AtomicLong();
        TokenSinker tsinker = new TokenSinker(splitNodes.size());
        Flux<Flux<T>> fcmf = Flux.fromIterable(qp.channels)
        .map(channelName -> {
            long channelIx = oChannels.getAndAdd(1);
            LOGGER.debug("{}  buildMerged next channel {}", reqctx, channelName);
            return Flux.fromIterable(splitNodes)
            .subscribeOn(clientsplitnodeiter)
            .filter(sn -> qp.splits == null || qp.splits.isEmpty() || qp.splits.contains(sn.split))
            .doOnNext(sn -> {
                LOGGER.debug("{}  buildMerged next split node  sn {} {}  sp {}", reqctx, sn.host, sn.port, sn.split);
            })
            .zipWith(Flux.<Integer>create(sink -> {
                AtomicLong tState = new AtomicLong();
                sink.onRequest(reqno -> {
                    long stn = tState.getAndAdd(1);
                    if (stn < 0 || stn > splitNodes.size()) {
                        LOGGER.error("{}  Too many sub-requests {}", reqctx, stn);
                        throw new RuntimeException("logic");
                    }
                    if (reqno != 1) {
                        throw new RuntimeException("bad request limit");
                    }
                    new Thread(() -> {
                        try {
                            int a = tsinker.queue.take();
                            sink.next(a);
                        }
                        catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }).start();
                });
                sink.onCancel(() -> {
                    LOGGER.debug("{}  Token flux cancelled", reqctx);
                });
                sink.onDispose(() -> {
                    LOGGER.debug("{}  Dispose of token flux", reqctx);
                });
            })
            .doOnDiscard(Integer.class, tok -> tsinker.putBack(tok))
            .limitRate(1)
            .publishOn(clientsplitnodeiter),
            1)
            .concatMap(tok -> {
                SplitNode sn = tok.getT1();
                Integer token = tok.getT2();
                LOGGER.debug("{}  buildMerged Query  {}  {}/{}  sn {} {}  sp {}  token {}", reqctx, channelName, channelIx, nChannels, sn.host, sn.port, sn.split, token);
                String localURL = String.format("http://%s:%d/api/1/rawLocal", sn.host, sn.port);
                Query subq = new Query();
                subq.decompressOnServer = 0;
                subq.bufferSize = qp.bufferSize;
                subq.channels = List.of(channelName);
                Range range = new Range();
                range.startDate = qp.beginString;
                range.endDate = qp.endString;
                subq.range = range;
                subq.splits = List.of(sn.split);
                String js;
                try {
                    ObjectMapper mapper = new ObjectMapper(new JsonFactory());
                    js = mapper.writeValueAsString(subq);
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
                AtomicLong skipInResponse = new AtomicLong(4);
                Mono<Flux<Item>> m3 = springWebClientRequest(reqctx, localURL, channelName, js)
                .map(clientResponse -> {
                    LOGGER.debug(logMarkerWebClientResponse, "{}  WebClient  create body flux  channel {}  sn {} {}  sp {}", reqctx, channelName, sn.host, sn.port, sn.split);
                    String remote_reqid = clientResponse.headers().header("x-daqbuffer-request-id").get(0);
                    return Tuples.of(remote_reqid, clientResponse.bodyToFlux(DataBuffer.class));
                })
                .<Flux<Item>>map(tup -> {
                    String remote_reqid = tup.getT1();
                    Flux<DataBuffer> flbuf = tup.getT2();
                    return flbuf.doOnSubscribe(s -> {
                        LOGGER.debug(logMarkerWebClientResponse, "{}  WebClient Response  {}  {}  {}  SUBSCRIBE", reqctx, channelName, sn.host, sn.split);
                    })
                    .doOnNext(buf -> {
                        int readable = buf.readableByteCount();
                        long skip = skipInResponse.get();
                        LOGGER.trace(logMarkerWebClientResponseItem, "{}  WebClient Response  {}  {}  {}  NEXT   skip-A {} / {}", reqctx, channelName, sn.host, sn.split, skip, readable);
                        if (skip > 0) {
                            if (skip > readable) {
                                skip = readable;
                            }
                            buf.readPosition(buf.readPosition() + (int) skip);
                            skipInResponse.getAndAdd(-skip);
                        }
                    })
                    .onErrorResume(e -> {
                        LOGGER.error("{}  WebClient error, call getRequestStatus", reqctx);
                        return getRequestStatus(reqctx, sn.host, sn.port, remote_reqid)
                        .flatMapMany(res -> {
                            if (res.requestStatus != null) {
                                requestStatusBoard.getOrCreate(reqctx).addSubRequestStatus(res.requestStatus);
                            }
                            else {
                                String s1 = StandardCharsets.UTF_8.decode(res.byteBuffer).toString();
                                requestStatusBoard.getOrCreate(reqctx).addError(new RequestStatus.Error(s1));
                            }
                            return Flux.error(new RuntimeException("subnode error"));
                        });
                    })
                    .doOnCancel(() -> {
                        LOGGER.info(logMarkerWebClientResponse, "{}  WebClient Response  {}  {}  {}  CANCEL", reqctx, channelName, sn.host, sn.split);
                    })
                    .doOnComplete(() -> {
                        LOGGER.debug(logMarkerWebClientResponse, "{}  WebClient Response  {}  {}  {}  COMPLETE", reqctx, channelName, sn.host, sn.split);
                        tsinker.putBack(token);
                    })
                    .doOnTerminate(() -> {
                        LOGGER.debug(logMarkerWebClientResponse, "{}  WebClient Response  {}  {}  {}  TERMINATE", reqctx, channelName, sn.host, sn.split);
                    })
                    .timeout(Duration.ofMillis(40000))
                    .onErrorMap(TimeoutException.class, e -> {
                        LOGGER.error("{}  Timeout1  channel {}  {} {}  {}", reqctx, channelName, sn.host, sn.port, sn.split);
                        return new Timeout1();
                    })
                    .transform(fbuf2 -> EventBlobToV1MapTs.trans2(reqctx, EventBlobToV1MapTs.Mock.NONE, fbuf2, String.format("__sn_%02d__buildMerged__%s", sn.split, channelName), channelName, qp.endNanos, qp.bufFac, qp.bufferSize))
                    .timeout(Duration.ofMillis(60000))
                    .onErrorMap(TimeoutException.class, e -> {
                        LOGGER.error("{}  Timeout2  channel {}  {} {}  {}", reqctx, channelName, sn.host, sn.port, sn.split);
                        return new Timeout2();
                    })
                    .doOnComplete(() -> {
                        LOGGER.debug("{}  after EventBlobToV1MapTs  {}  {}  COMPLETE", reqctx, channelName, sn.host);
                    })
                    .doOnTerminate(() -> {
                        LOGGER.debug("{}  after EventBlobToV1MapTs  {}  {}  TERMINATE", reqctx, channelName, sn.host);
                    })
                    .transform(fl3 -> logFlux(reqctx, String.format("merged_sn%02d_ts_flux_ts", sn.split), fl3))
                    .concatWith(Mono.defer(() -> {
                        return getRequestStatus(reqctx, sn.host, sn.port, remote_reqid)
                        .flatMap(res -> {
                            if (res.requestStatus != null) {
                                requestStatusBoard.getOrCreate(reqctx).addSubRequestStatus(res.requestStatus);
                            }
                            else {
                                String s1 = StandardCharsets.UTF_8.decode(res.byteBuffer).toString();
                                requestStatusBoard.getOrCreate(reqctx).addError(new RequestStatus.Error(s1));
                            }
                            return Mono.<Item>empty();
                        });
                    }));
                });
                return logMono(reqctx, String.format("merged_sn%02d_ts_mono_sub", sn.split), m3);
            })
            .collectList()
            .<Flux<DataBuffer>>map(lfl -> pipeThroughMerger(reqctx, lfl, channelName, qp))
            .flatMapMany(fl12 -> transformSupplier.trans3(fl12, channelName))
            .doOnSubscribe(s -> {
                LOGGER.debug("{}  merged stream SUBSCRIBE  {}", reqctx, channelName);
            })
            .doOnCancel(() -> {
                LOGGER.info("{}  merged stream CANCEL     {}", reqctx, channelName);
            })
            .doOnComplete(() -> {
                LOGGER.debug("{}  merged stream COMPLETE   {}", reqctx, channelName);
            })
            .doOnTerminate(() -> {
                LOGGER.debug("{}  merged stream TERMINATE  {}", reqctx, channelName);
            })
            .timeout(Duration.ofMillis(80000))
            .onErrorMap(TimeoutException.class, e -> {
                LOGGER.error("{}  Timeout3  channel {}", reqctx, channelName);
                return new Timeout3();
            });
        });
        return fcmf
        .<T>concatMap(x -> x, 1);
    }

    static class TransMapQueryMerged implements TransformSupplier<DataBuffer> {
        QueryParams qp;
        ReqCtx reqctx;
        TransMapQueryMerged(ReqCtx reqctx, QueryParams qp) {
            this.reqctx = reqctx;
            this.qp = qp;
        }
        public Flux<DataBuffer> trans3(Flux<DataBuffer> fl, String channelName) {
            return EventBlobToV1Map.trans2(reqctx, fl, channelName, qp.endNanos, qp.bufFac, qp.bufferSize, qp.decompressOnServer, qp.limitBytes)
            .map(res -> {
                if (res.buf == null) {
                    LOGGER.error("{} BAD: res.buf == null", reqctx);
                    throw new RuntimeException("BAD: res.buf == null");
                }
                return res.buf;
            });
        }
    }

    static class TransMapQueryMergedFake implements TransformSupplier<DataBuffer> {
        QueryParams qp;
        ReqCtx reqctx;
        TransMapQueryMergedFake(ReqCtx reqctx, QueryParams qp) {
            this.reqctx = reqctx;
            this.qp = qp;
        }
        public Flux<DataBuffer> trans3(Flux<DataBuffer> fl, String channelName) {
            return fl;
        }
    }

    public Mono<ResponseEntity<Flux<DataBuffer>>> queryMergedOctets(ReqCtx reqctx, ServerWebExchange exchange, @RequestBody Mono<Query> queryMono) {
        requestStatusBoard.requestBegin(reqctx);
        ServerHttpRequest req = exchange.getRequest();
        if (!req.getHeaders().getAccept().contains(MediaType.APPLICATION_OCTET_STREAM)) {
            LOGGER.warn("{}  queryMerged  Client omits Accept: {}", reqctx, MediaType.APPLICATION_OCTET_STREAM);
        }
        Mono<Flux<DataBuffer>> mret = queryMono
        .doOnError(x -> LOGGER.error("{}  can not parse request", reqctx))
        .map(query -> {
            QueryParams qp = QueryParams.fromQuery(query, exchange.getResponse().bufferFactory(), bufferSize);
            LOGGER.debug("{}  queryMerged  {}  {}  {}", reqctx, qp.begin, qp.end, qp.channels);
            AtomicLong nbytes = new AtomicLong();
            return buildMerged(reqctx, qp, new TransMapQueryMerged(reqctx, qp))
            .doOnSubscribe(nreq1 -> {
                LOGGER.trace("{}  doOnSubscribe  queryMergedOctets: subscribing to result from buildMerged", reqctx);
            })
            .doOnNext(buf -> {
                if (buf == null) {
                    LOGGER.error("{}  buf is null, can not add readable byte count", reqctx);
                    throw new RuntimeException("logic");
                }
                else {
                    reqctx.addBodyLen(buf);
                    LOGGER.trace(logMarkerQueryMergedItems, "{}  queryMerged  net emit  len {}", reqctx, buf.readableByteCount());
                    long h = nbytes.addAndGet(buf.readableByteCount());
                    if (query.errorAfterBytes > 0 && h > query.errorAfterBytes) {
                        throw new RuntimeException("Byte limit reached");
                    }
                }
            })
            .doOnComplete(() -> {
                LOGGER.debug("{}  queryMerged COMPLETE", reqctx);
            })
            .doOnTerminate(() -> {
                requestStatusBoard.bodyEmitted(reqctx);
                LOGGER.debug("{}  queryMerged TERMINATE", reqctx);
                LOGGER.info("RequestStatus bodyEmitted summary {}", requestStatusBoard.getOrCreate(reqctx).summary());
            })
            .doOnError(e -> {
                LOGGER.error("{}  queryMerged ERROR  {}", reqctx, e.toString());
                requestStatusBoard.getOrCreate(reqctx).addError(new RequestStatus.Error(e.toString()));
            });
        });
        return logResponse(reqctx, "queryMerged", mret);
    }

    static class MakeTrans2 implements MapFunctionFactory<DataBuffer> {
        @Override
        public Flux<DataBuffer> makeTrans(Flux<DataBuffer> fl, KeyspaceToDataParams kspp, int fileno) {
            return fl;
        }
    }

    public Mono<ResponseEntity<Flux<DataBuffer>>> queryMergedOctetsLocal(ReqCtx reqctx, ServerWebExchange exchange, @RequestBody Mono<Query> queryMono) {
        requestStatusBoard.requestBegin(reqctx);
        Mono<Flux<DataBuffer>> mret = queryMono
        .doOnError(x -> LOGGER.error("{}  can not parse request", reqctx))
        .map(query -> {
            QueryParams qp = QueryParams.fromQuery(query, exchange.getResponse().bufferFactory(), bufferSize);
            LOGGER.debug("{}  queryMergedLocal  {}  {}  {}", reqctx, qp.begin, qp.end, qp.channels);
            Instant begin = Instant.parse(query.range.startDate);
            Instant end = Instant.parse(query.range.endDate);
            long endNanos = 1000000L * end.toEpochMilli();
            Flux<Mono<Flux<EventBlobMapResult>>> fcmf = Flux.fromIterable(query.channels)
            .map(channelName -> {
                Flux<Mono<Flux<Item>>> fmf = Flux.fromIterable(splitNodes)
                .filter(sn -> qp.splits == null || qp.splits.isEmpty() || qp.splits.contains(sn.split))
                .map(sn -> {
                    LOGGER.debug("{}  local split {}", reqctx, sn.split);
                    Query subq = new Query();
                    subq.decompressOnServer = 0;
                    subq.bufferSize = qp.bufferSize;
                    subq.channels = List.of(channelName);
                    subq.range = query.range;
                    subq.splits = List.of(sn.split);
                    Function<KeyspaceToDataParams, Mono<List<Flux<DataBuffer>>>> keyspaceToData = p -> {
                        return ChannelEventStream.dataFluxFromFiles(p, new MakeTrans2());
                    };
                    Flux<DataBuffer> fbuf = channelsToData(reqctx, exchange, baseDirFinder, subq.channels, qp.begin, qp.end, subq.splits, qp.bufFac, qp.bufferSize, keyspaceToData)
                    .doOnDiscard(DataBuffer.class, DataBufferUtils::release);
                    Flux<Item> flItem = EventBlobToV1MapTs.trans2(reqctx, EventBlobToV1MapTs.Mock.NONE, fbuf, String.format("__sn%02d__QML", sn.split), channelName, endNanos, qp.bufFac, qp.bufferSize);
                    return Mono.just(flItem);
                    /*
                    return logMono(String.format("merged_sn%02d_ts_mono_sub", sn.split), req, m3);
                    */
                });
                Flux<Mono<Flux<Item>>> fmf2 = logFlux(reqctx, "merged_ts_flux_subs", fmf);
                Mono<Flux<EventBlobMapResult>> fl4 = fmf2.concatMap(Function.identity(), 1)
                .map(fl -> fl.doOnDiscard(DataBuffer.class, DataBufferUtils::release))
                .collectList()
                .map(lfl -> Flux.from(new Merger(reqctx, channelName, lfl, qp.bufFac, qp.bufferSize)))
                .map(flbuf -> {
                    return EventBlobToV1Map.trans2(reqctx, flbuf, channelName, endNanos, qp.bufFac, qp.bufferSize, qp.decompressOnServer, qp.limitBytes)
                    .doOnNext(x2 -> {
                        if (x2.term) {
                            LOGGER.warn("{}  EventBlobToV1Map reached TERM", reqctx);
                        }
                    })
                    .takeWhile(x2 -> !x2.term);
                });
                return fl4;
            });
            return fcmf.<Flux<EventBlobMapResult>>concatMap(x -> x, 1)
            .<EventBlobMapResult>concatMap(x -> x, 1)
            .<DataBuffer>map(x -> x.buf)
            .doOnNext(buf -> reqctx.addBodyLen(buf))
            .doOnTerminate(() -> {
                requestStatusBoard.bodyEmitted(reqctx);
                LOGGER.info("RequestStatus bodyEmitted summary {}", requestStatusBoard.getOrCreate(reqctx).summary());
            });
        });
        return logResponse(reqctx, "queryMergedLocal", mret);
    }

    static class TransformSup3 implements TransformSupplier<MapJsonResult> {
        QueryParams qp;
        TransformSup3(QueryParams qp) {
            this.qp = qp;
        }
        public Flux<MapJsonResult> trans3(Flux<DataBuffer> fl, String channelName) {
            return EventBlobToJsonMap.trans2(fl, channelName, qp.endNanos, qp.bufFac, qp.bufferSize, qp.limitBytes);
        }
    }

    public Mono<ResponseEntity<Flux<DataBuffer>>> queryMergedJson(ReqCtx reqctx, ServerWebExchange exchange, @RequestBody Mono<Query> queryMono) {
        requestStatusBoard.requestBegin(reqctx);
        ServerHttpRequest req = exchange.getRequest();
        if (!req.getHeaders().getAccept().contains(MediaType.APPLICATION_JSON)) {
            LOGGER.warn("{}  queryMerged  Client omits Accept: {}", req.getId(), MediaType.APPLICATION_JSON);
            //throw new RuntimeException("Incompatible Accept header");
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
            QueryParams qp = QueryParams.fromQuery(query, defaultDataBufferFactory, bufferSize);
            LOGGER.info(String.format("%s  queryMerged  %s  %s  %s", req.getId(), qp.begin, qp.end, qp.channels));
            AggMapper mapper = new AggMapper(binFind, aggs, qp.bufFac);
            return buildMerged(reqctx, qp, new TransformSup3(qp))
            .map(mapper::map)
            .concatWith(Mono.defer(() -> Mono.just(mapper.finalResult())))
            .concatMapIterable(Function.identity(), 1)
            .doOnNext(buf -> {
                reqctx.addBodyLen(buf);
                totalBytesServed.getAndAdd(buf.readableByteCount());
            })
            .doOnTerminate(() -> {
                mapper.release();
                requestStatusBoard.bodyEmitted(reqctx);
                LOGGER.info("RequestStatus bodyEmitted summary {}", requestStatusBoard.getOrCreate(reqctx).summary());
            });
        });
        return logMono(reqctx, "queryMergedJson", mret.map(x -> {
            return ResponseEntity.ok()
            .header("X-CanonicalHostname", canonicalHostname)
            .contentType(MediaType.APPLICATION_JSON)
            .body(x);
        }));
    }

    static <T> Mono<T> logMono(ReqCtx reqctx, String name, Mono<T> m) {
        if (true) return m;
        return m
        .doOnSuccess(x -> {
            LOGGER.debug("{}  success  {}", reqctx, name);
        })
        .doOnCancel(() -> {
            LOGGER.info("{}  cancel  {}", reqctx, name);
        })
        .doOnError(e -> {
            LOGGER.error("{}  error  {}  {}", reqctx, name, e.toString());
        })
        .doOnTerminate(() -> {
            LOGGER.debug("{}  terminate  {}", reqctx, name);
        });
    }

    static <T> Flux<T> logFlux(ReqCtx reqctx, String name, Flux<T> m) {
        if (true) return m;
        return m
        .doOnComplete(() -> {
            LOGGER.debug("{}  complete  {}", reqctx, name);
        })
        .doOnCancel(() -> {
            LOGGER.info("{}  cancel  {}", reqctx, name);
        })
        .doOnError(e -> {
            LOGGER.error("{}  error  {}  {}", reqctx, name, e.toString());
        })
        .doOnTerminate(() -> {
            LOGGER.debug("{}  terminate  {}", reqctx, name);
        });
    }

    <T> Mono<ResponseEntity<Flux<T>>> logResponse(ReqCtx reqctx, String name, Mono<Flux<T>> m) {
        return logMono(reqctx, name, m.map(x -> {
            return ResponseEntity.ok()
            .header("X-CanonicalHostname", canonicalHostname)
            .header("x-daqbuffer-request-id", reqctx.reqId)
            .contentType(MediaType.APPLICATION_OCTET_STREAM)
            .body(x);
        }));
    }

    static Scheduler fs = Schedulers.newParallel("fs", 1);

    <T> Flux<T> channelsToData(ReqCtx reqctx, ServerWebExchange exchange, BaseDirFinderFormatV0 baseDirFinder, List<String> channels, Instant begin, Instant end, List<Integer> splits, DataBufferFactory bufFac, int bufferSize, Function<KeyspaceToDataParams, Mono<List<Flux<T>>>> keyspaceToData) {
        Flux<T> ret = Flux.fromIterable(channels)
        .subscribeOn(fs)
        .concatMap(channelName -> {
            Flux<T> bulk = baseDirFinder.findMatchingDataFiles(reqctx, channelName, begin, end, splits, bufFac)
            .doOnNext(x -> {
                if (x.keyspaces.size() < 1) {
                    LOGGER.warn(String.format("no keyspace found for channel %s", channelName));
                }
                else if (x.keyspaces.size() > 1) {
                    LOGGER.warn(String.format("more than one keyspace for %s", channelName));
                }
                else {
                    if (false) {
                        LOGGER.info("Channel {} using files: {}", channelName, x.keyspaces.get(0).splits.stream().map(sp -> sp.timeBins.size()).reduce(0, (a2, x2) -> a2 + x2));
                    }
                }
            })
            .flatMapIterable(x2 -> x2.keyspaces)
            .concatMap(ks -> keyspaceToData.apply(new KeyspaceToDataParams(reqctx, ks, begin, end, bufFac, bufferSize, splits, exchange.getRequest())), 1)
            .concatMap(x -> {
                if (x.size() <= 0) {
                    throw new RuntimeException("logic");
                }
                if (x.size() > 1) {
                    throw new RuntimeException("not yet supported in local query");
                }
                return x.get(0);
            }, 1);
            return bulk;
        }, 1);
        Flux<T> ret2 = logFlux(reqctx, "channelsToData", ret);
        return ret2;
    }

}
