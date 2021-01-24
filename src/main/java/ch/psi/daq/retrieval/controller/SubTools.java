package ch.psi.daq.retrieval.controller;

import ch.psi.daq.retrieval.QueryParams;
import ch.psi.daq.retrieval.ReqCtx;
import ch.psi.daq.retrieval.bytes.BufCont;
import ch.psi.daq.retrieval.config.ConfigurationRetrieval;
import ch.psi.daq.retrieval.config.Node;
import ch.psi.daq.retrieval.error.ErrorUtils;
import ch.psi.daq.retrieval.error.RequestException;
import ch.psi.daq.retrieval.eventmap.ts.EventBlobToV1MapTs;
import ch.psi.daq.retrieval.eventmap.ts.MapTsItemVec;
import ch.psi.daq.retrieval.eventmap.value.TransformSupplier;
import ch.psi.daq.retrieval.pod.api1.Query;
import ch.psi.daq.retrieval.pod.api1.Range;
import ch.psi.daq.retrieval.status.RequestStatusBoard;
import ch.psi.daq.retrieval.status.RequestStatusFetch;
import ch.qos.logback.classic.Logger;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import static ch.psi.daq.retrieval.controller.QueryData.logMarkerWebClientResponse;
import static ch.psi.daq.retrieval.controller.QueryData.logMarkerWebClientResponseItem;

public class SubTools {
    static final Logger LOGGER = (Logger) LoggerFactory.getLogger(SubTools.class.getSimpleName());


    static <T> Flux<T> buildMerged(ReqCtx reqCtx, QueryParams qp, TransformSupplier<T> transformSupplier, QueryData.MergeFunction mergeFunction, RequestStatusBoard requestStatusBoard, ConfigurationRetrieval conf) {
        LOGGER.info("buildMerged  {}  {}", qp.begin, qp.end);
        final int nChannels = qp.channels.size();
        final AtomicLong oChannels = new AtomicLong();
        TokenSinker tsinker = new TokenSinker(conf.nodes.size());
        return Flux.fromIterable(qp.channels)
        .concatMap(channelName -> {
            long channelIx = oChannels.getAndAdd(1);
            LOGGER.debug("{}  buildMerged next channel {}", reqCtx, channelName);
            int tokenRate = qp.subTokenRate + 1;
            Flux<Integer> tokens = Flux.<Integer>create(sink -> {
                AtomicLong tState = new AtomicLong();
                sink.onRequest(reqno -> {
                    long stn = tState.getAndAdd(1);
                    if (stn < 0 || stn > conf.nodes.size()) {
                        LOGGER.error("{}  Too many sub-requests {}", reqCtx, stn);
                        throw new RuntimeException("logic");
                    }
                    if (reqno <= 0 || reqno > tokenRate) {
                        throw new RuntimeException(String.format("bad request limit %d", reqno));
                    }
                    Thread thread = new Thread(() -> {
                        try {
                            int a = tsinker.queue.take();
                            sink.next(a);
                        }
                        catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    });
                    thread.setName(String.format("tsinker-%d-%s", System.nanoTime()/1000/1000, channelName));
                    thread.start();
                });
                sink.onCancel(() -> {
                    LOGGER.debug("{}  Token flux cancelled", reqCtx);
                });
                sink.onDispose(() -> {
                    LOGGER.debug("{}  Dispose of token flux", reqCtx);
                });
            })
            .doOnDiscard(Integer.class, tsinker::putBack)
            .limitRate(tokenRate);
            List<Node> nodes;
            if (qp.splits != null) {
                nodes = conf.nodes.stream()
                .filter(k -> qp.splits.stream().anyMatch(kk -> k.splits.contains(kk)))
                .collect(Collectors.toList());
            }
            else {
                nodes = conf.nodes;
            }
            return Flux.fromIterable(nodes)
            .subscribeOn(QueryData.clientsplitnodeiter)
            .zipWith(tokens, 1)
            .concatMap(tok -> {
                Node node = tok.getT1();
                Integer token = tok.getT2();
                String localURL = String.format("http://%s:%d/api/1/rawLocal", node.host, node.port);
                Query subq = new Query();
                subq.bufferSize = qp.bufferSize;
                subq.channels = List.of(channelName);
                Range range = new Range();
                range.startDate = qp.beginString;
                range.endDate = qp.endString;
                subq.range = range;
                if (qp.splits != null) {
                    subq.splits = qp.splits.stream().filter(k -> node.splits.contains(k)).collect(Collectors.toList());
                }
                else {
                    subq.splits = node.splits;
                }
                subq.limitBytesPerChannel = qp.limitBytesPerChannel;
                LOGGER.debug("{}  buildMerged Query  channels {}  splits {}  {}/{}  sn {} {}  token {}", reqCtx, subq.channels, subq.splits, channelIx, nChannels, node.host, node.port, token);
                String js;
                Function<Object, String> writeString = k -> {
                    try {
                        ObjectMapper om = new ObjectMapper(new JsonFactory());
                        return om.writeValueAsString(subq);
                    }
                    catch (JsonProcessingException e) {
                        return null;
                    }
                };
                js = writeString.apply(subq);
                AtomicLong skipInResponse = new AtomicLong(4);
                Mono<SubStream> sub;
                if (qp.subreqType == 1) {
                    sub = springWebClientRequest(reqCtx, localURL, channelName, js);
                }
                else if (qp.subreqType == 2) {
                    sub = springWebClientRequestFake(reqCtx, localURL, channelName, js);
                }
                else if (qp.subreqType == 3) {
                    sub = RawClient.rawSub(subq, channelName, node);
                }
                else {
                    sub = RawClient.rawSub(subq, channelName, node);
                }
                Mono<Flux<MapTsItemVec>> m3 = sub
                .doOnSubscribe(k -> LOGGER.info("subscribe to sub  {}  {}", node.host, channelName))
                .map(subres -> {
                    String remote_reqid = subres.reqId;
                    AtomicLong tsL = new AtomicLong(System.nanoTime());
                    return subres.fl
                    .doOnNext(k -> {
                        long ts = System.nanoTime();
                        long dt = ts - tsL.get();
                        if (dt > 1000L * 1000 * 1000 * 20) {
                            LOGGER.warn("{}  WebClient Response  long delay {}  {}  {}", reqCtx, dt, node.host, channelName);
                        }
                        tsL.set(ts);
                    })
                    .doOnSubscribe(s -> {
                        LOGGER.debug(logMarkerWebClientResponse, "{}  WebClient Response  {}  {}  {}  SUBSCRIBE", reqCtx, channelName, node.host, node.port);
                    })
                    .doOnNext(buf -> {
                        int readable = buf.readableByteCount();
                        long skip = skipInResponse.get();
                        LOGGER.trace(logMarkerWebClientResponseItem, "{}  WebClient Response  {}  {}  {}  NEXT   skip-A {} / {}", reqCtx, channelName, node.host, node.port, skip, readable);
                        if (skip > 0) {
                            if (skip > readable) {
                                skip = readable;
                            }
                            buf.readPosition(buf.readPosition() + (int) skip);
                            skipInResponse.getAndAdd(-skip);
                        }
                    })
                    .map(k -> BufCont.fromBuffer(k, BufCont.Mark.BUILD_MERGED_FROM_SUBQUERY))
                    .onErrorResume(e -> {
                        LOGGER.error("{}  WebClient error, call getRequestStatus", reqCtx);
                        return RequestStatusFetch.getRequestStatus(requestStatusBoard, reqCtx, node.host, node.port, remote_reqid).then(Mono.empty());
                    })
                    .doOnCancel(() -> {
                        LOGGER.info(logMarkerWebClientResponse, "{}  WebClient Response  {}  {}  {}  {}  CANCEL", reqCtx, channelName, node.host, node.port, remote_reqid);
                    })
                    .doFinally(sig -> {
                        tsinker.putBack(token);
                    })
                    //.transform(doDiscard("buildMergedMiddle1"))
                    .transform(fbuf2 -> EventBlobToV1MapTs.trans(reqCtx, fbuf2, String.format("__sn_%02d__buildMerged__%s", node.splits.get(0), channelName), channelName, qp.endNanos, reqCtx.bufCtx))
                    .doOnComplete(() -> {
                        LOGGER.debug("{}  after EventBlobToV1MapTs  {}  {}  COMPLETE", reqCtx, channelName, node.host);
                    })
                    .doOnTerminate(() -> {
                        LOGGER.debug("{}  after EventBlobToV1MapTs  {}  {}  TERMINATE", reqCtx, channelName, node.host);
                    })
                    .onErrorMap(e -> {
                        LOGGER.error("{}  buildMerged after EventBlobToV1MapTs  subreq {}  {}  {}\n{}", reqCtx, remote_reqid, node.host, channelName, ErrorUtils
                        .traceString(e));
                        return new RequestException(reqCtx, "request error");
                    })
                    .doOnNext(kk -> kk.markWith(BufCont.Mark.QUERY_DATA_MERGED_01))
                    .concatWith(Mono.defer(() -> {
                        return RequestStatusFetch.getRequestStatus(requestStatusBoard, reqCtx, node.host, node.port, remote_reqid).then(Mono.empty());
                    }))
                    .doOnNext(kk -> kk.markWith(BufCont.Mark.QUERY_DATA_MERGED_02))
                    .transform(QueryData.doDiscard("buildMergedMiddle2"));
                });
                return m3;
            }, qp.prepareSubfluxPrefetch)
            .collectList()
            .doOnNext(k -> LOGGER.info("-----------   Next batch of sub fluxes  {}", k.size()))
            .transform(QueryData.doDiscardMono("buildMergedBeforeMerger"))
            .<Flux<BufCont>>map(lfl -> mergeFunction.apply(reqCtx, lfl, channelName, qp, reqCtx.bufCtx))
            .onErrorMap(e -> {
                LOGGER.error("{}  buildMerged after pipeThroughMerger:\n{}", reqCtx, ErrorUtils.traceString(e));
                return new RequestException(reqCtx, "request error");
            })
            .map(k -> k.doOnNext(kk -> kk.appendMark(BufCont.Mark.QUERY_DATA_MERGED_04)))
            .flatMapMany(fl12 -> transformSupplier.trans3(fl12, channelName))
            .doFinally(sig -> {
                LOGGER.info("{}  merged stream final  {}  {}", reqCtx, sig, channelName);
            });
        }, 0);
    }


    static Mono<SubStream> springWebClientRequest(ReqCtx reqCtx, String localURL, String channelName, String js) {
        Flux<DataBuffer> fl = WebClient.builder()
        .baseUrl(localURL)
        .build()
        .post()
        .contentType(MediaType.APPLICATION_JSON)
        .accept(MediaType.APPLICATION_OCTET_STREAM)
        .header("connection", "close")
        .header("x-main-req-id", reqCtx.reqId)
        .body(BodyInserters.fromValue(js))
        .exchangeToFlux(res -> {
            if (res.statusCode() != HttpStatus.OK) {
                LOGGER.error("{}  WebClient got status {}  {}", reqCtx, res.statusCode(), channelName);
                return Flux.error(new RuntimeException("sub request not OK"));
            }
            else {
                return res.bodyToFlux(DataBuffer.class);
            }
        });
        SubStream ret = new SubStream();
        ret.reqId = "no_remote_id";
        ret.fl = fl;
        return Mono.just(ret);
        /*
        .exchange()
        .flatMap(res -> {
            if (res.statusCode() != HttpStatus.OK) {
                LOGGER.error("{}  WebClient got status {}  {}", reqCtx, res.statusCode(), channelName);
                return Mono.error(new RuntimeException("sub request not OK"));
            }
            else {
                if (!res.headers().asHttpHeaders().containsKey(HEADER_X_DAQBUFFER_REQUEST_ID)) {
                    LOGGER.error("{}  subnode request returned no request id", reqCtx);
                    return Mono.error(new RuntimeException("no reqid from node"));
                }
                else {
                    String remoteReqId = res.headers().header(HEADER_X_DAQBUFFER_REQUEST_ID).get(0);
                    if (!res.headers().header("connection").contains("close")) {
                        LOGGER.error("{}  WebClient no conn close header  {}", reqCtx, channelName);
                        for (Map.Entry<String, List<String>> e : res.headers().asHttpHeaders().entrySet()) {
                            LOGGER.error("{}  header: {}", reqCtx, e.getKey());
                            for (String v : e.getValue()) {
                                LOGGER.error("{}   v: {}", reqCtx, v);
                            }
                        }
                        LOGGER.error("{}  {}", reqCtx, res.headers().toString());
                    }
                    SubStream ret = new SubStream();
                    ret.reqId = remoteReqId;
                    ret.fl = res.bodyToFlux(DataBuffer.class)
                    //.map(k -> reqCtx.bufCtx.bufFac.wrap(k))
                    //.doOnDiscard(DataBuffer.class, DataBufferUtils::release)
                    ;
                    return Mono.just(ret);
                }
            }
        });
        */
    }

    static Mono<SubStream> springWebClientRequestFake(ReqCtx reqCtx, String localURL, String channelName, String js) {
        SubStream ret = new SubStream();
        ret.reqId = "dummy";
        ret.fl = Flux.generate(() -> GenSt.create(reqCtx.bufCtx), GenSt::next, GenSt::release);
        return Mono.just(ret);
    }

}
