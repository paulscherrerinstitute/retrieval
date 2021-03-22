package ch.psi.daq.retrieval.subnodes;

import ch.psi.daq.retrieval.eventmap.ts.MapTsItemVec;
import ch.psi.daq.retrieval.merger.CompleteEvent;
import ch.psi.daq.retrieval.merger.MergerSupport;
import ch.psi.daq.retrieval.utils.DateExt;
import ch.psi.daq.retrieval.QueryParams;
import ch.psi.daq.retrieval.reqctx.ReqCtx;
import ch.psi.daq.retrieval.bytes.BufCont;
import ch.psi.daq.retrieval.config.ChannelConfigEntry;
import ch.psi.daq.retrieval.config.ConfigurationRetrieval;
import ch.psi.daq.retrieval.config.Node;
import ch.psi.daq.retrieval.controller.Parts;
import ch.psi.daq.retrieval.controller.QueryData;
import ch.psi.daq.retrieval.controller.mergefunc.MergeFunction;
import ch.psi.daq.retrieval.error.ErrorUtils;
import ch.psi.daq.retrieval.error.RequestException;
import ch.psi.daq.retrieval.eventmap.ts.EventBlobToV1MapTs;
import ch.psi.daq.retrieval.eventmap.ts.MapTsToken;
import ch.psi.daq.retrieval.pod.api1.Channel;
import ch.psi.daq.retrieval.pod.api1.ChannelList;
import ch.psi.daq.retrieval.pod.api1.Query;
import ch.psi.daq.retrieval.pod.api1.Range;
import ch.psi.daq.retrieval.status.RequestStatusBoard;
import ch.psi.daq.retrieval.status.RequestStatusFetch;
import ch.psi.daq.retrieval.utils.Tools;
import ch.qos.logback.classic.Logger;
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

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class SubTools {
    static final Logger LOGGER = (Logger) LoggerFactory.getLogger(SubTools.class.getSimpleName());

    public static class MergedChannelBuilt {
        public ChannelConfigEntry configEntry;
        public Flux<MapTsToken> fl;
        public ConcurrentLinkedQueue<String> subReqIds;
        public ConcurrentLinkedQueue<Node> subNodes;
    }

    public static <T> Mono<MergedChannelBuilt> buildMergedChannel(ReqCtx reqCtx, Channel channel, QueryParams qp, Instant beg, Instant end, int trailingEventsMax, MergeFunction mergeFunction, RequestStatusBoard requestStatusBoard, ConfigurationRetrieval conf) {
        LOGGER.debug("{}  buildMergedChannel  {}  {}  {}", reqCtx, beg, end, channel.name);
        long endNanos = DateExt.toLong(end);
        List<Node> nodes;
        if (qp.splits != null) {
            nodes = conf.nodes.stream()
            .filter(k -> qp.splits.stream().anyMatch(kk -> k.splits.contains(kk)))
            .collect(Collectors.toList());
        }
        else {
            nodes = conf.nodes;
        }
        ConcurrentLinkedQueue<String> subReqIds = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Node> subNodes = new ConcurrentLinkedQueue<>();
        return Flux.fromIterable(nodes)
        .subscribeOn(QueryData.clientsplitnodeiter)
        .publishOn(QueryData.clientsplitnodeiter, 1)
        .concatMap(node -> {
            LOGGER.debug("{}  buildMergedChannel  prepare request  to {}  channel {}", reqCtx, node.host, channel.name);
            String localURL = String.format("http://%s:%d/api/1/rawLocal", node.host, node.port);
            Query subq = new Query();
            subq.mainReqId = reqCtx.reqId;
            subq.channels = new ChannelList(List.of(channel));
            Range range = new Range();
            range.startDate = beg.atZone(ZoneOffset.UTC).format(DateExt.datefmt);
            range.endDate = end.atZone(ZoneOffset.UTC).format(DateExt.datefmt);
            subq.range = range;
            if (qp.splits != null) {
                subq.splits = qp.splits.stream().filter(k -> node.splits.contains(k)).collect(Collectors.toList());
            }
            else {
                subq.splits = node.splits;
            }
            subq.trailingEventsMax = 1;
            subq.bufferSize = reqCtx.bufCtx.bufferSize;
            subq.bufferSizeDiskRead = qp.bufferSizeDiskRead;
            subq.limitBytesPerChannel = qp.limitBytesPerChannel;
            subq.limitRawRead = qp.limitRawRead;
            subq.waitForData = 0;
            LOGGER.debug("{}  buildMerged Query  channels {}  splits {}  remote {}:{}", reqCtx, subq.channels, subq.splits, node.host, node.port);
            String jsT;
            try {
                ObjectMapper om = new ObjectMapper();
                jsT = om.writeValueAsString(subq);
            }
            catch (JsonProcessingException e) {
                return Mono.error(new RuntimeException(e));
            }
            String js = jsT;
            Mono<SubStream> sub;
            if (qp.subreqType == 1) {
                //sub = springWebClientRequest(reqCtx, localURL, channel.name, js);
                sub = null;
            }
            else if (qp.subreqType == 2) {
                //sub = springWebClientRequestFake(reqCtx, localURL, channel.name, js);
                sub = null;
            }
            else {
                // type == 3
                sub = RawClient.rawClient(reqCtx, subq, channel.name, node, RawClient.Opts.create().wgroup(qp.subwgroup));
            }
            return sub
            .doOnSubscribe(k -> LOGGER.debug("{}  subscribe to sub  {}  {}", reqCtx, node.host, channel.name))
            .flatMap(subStream -> {
                Flux<BufCont> fl2 = subStream.fl.transform(QueryData.doDiscard("beforeParts"));
                Parts parts = Parts.fromPublisher(fl2, String.format("%s:%d", node.host, node.port));
                return subHeadRes(reqCtx, node, channel.name, Flux.from(parts))
                .map(shr -> {
                    SubresInter ret2 = new SubresInter();
                    ret2.subStream = subStream;
                    ret2.subHeadRes = shr;
                    ret2.parts = parts;
                    ret2.node = node;
                    return ret2;
                });
            });
        }, qp.prepareSubfluxPrefetch)
        .collectList()
        .doOnNext(k -> LOGGER.debug("-----------   Next batch of sub fluxes  {}", k.size()))
        .map(subs -> {
            ChannelConfigEntry ee = subs.get(0).subHeadRes.channelConfigEntry;
            List<Flux<MapTsToken>> subfl = subs.stream().map(q -> {
                subReqIds.add(q.subStream.reqId);
                subNodes.add(q.node);
                Node node = q.node;
                SubHeadRes head = q.subHeadRes;
                return Flux.fromIterable(head.bufs)
                .doOnNext(g -> g.appendMark(BufCont.Mark.SubTools3))
                .concatWith(q.parts)
                .doOnNext(g -> g.appendMark(BufCont.Mark.SubTools2))
                .transform(fbuf2 -> EventBlobToV1MapTs.trans(reqCtx, fbuf2, String.format("[tcp-res  host %s  channel %s]", node, channel), channel.name, endNanos, trailingEventsMax, reqCtx.bufCtx))
                .doOnComplete(() -> {
                    LOGGER.debug("{}  after EventBlobToV1MapTs  {}  {}  COMPLETE", reqCtx, channel, node);
                })
                .doOnTerminate(() -> {
                    LOGGER.debug("{}  after EventBlobToV1MapTs  {}  {}  TERMINATE", reqCtx, channel, node);
                })
                .onErrorMap(e -> {
                    LOGGER.error("{}  buildMerged after EventBlobToV1MapTs  subreq {}  {}  {}\n{}", reqCtx, q.subStream.reqId, node, channel, ErrorUtils.traceString(e));
                    return new RequestException(reqCtx, "error");
                })
                .concatMap(k -> k.intoFlux(0), 0)
                .transform(QueryData.doDiscard("buildMergedMiddle2"));
            }).collect(Collectors.toList());
            return new ConfigSubFlux(ee, subfl);
        })
        .map(q -> {
            AtomicLong pulseLastG = new AtomicLong(Long.MIN_VALUE);
            AtomicLong loggedGaps = new AtomicLong();
            Flux<MapTsToken> fl = mergeFunction.apply(reqCtx, q.fll, q.configEntry, channel, qp, reqCtx.bufCtx)
            .doOnNext(k -> {
                if (k.ty == MapTsItemVec.Ty.OPEN || k.ty == MapTsItemVec.Ty.FULL) {
                    long pulseLast = pulseLastG.get();
                    if (pulseLast != Long.MIN_VALUE && pulseLast + 1 < k.pulse) {
                        QueryData.subTools1GapCount.getAndAdd(1);
                        if (loggedGaps.getAndAdd(1) < 0) {
                            LOGGER.warn("{}  subTools1GapCount {}  pulseLast {}  pulse {}", reqCtx, QueryData.subTools1GapCount.get(), pulseLast, k.pulse);
                        }
                    }
                    pulseLastG.set(k.pulse);
                }
            })
            .onErrorMap(e -> {
                LOGGER.error("{}  buildMerged after MergerFunction:\n{}", reqCtx, ErrorUtils.traceString(e));
                return new RequestException(reqCtx, "request error");
            });
            MergedChannelBuilt ret = new MergedChannelBuilt();
            ret.configEntry = q.configEntry;
            ret.fl = fl;
            ret.subReqIds = subReqIds;
            ret.subNodes = subNodes;
            return ret;
        })
        .onErrorMap(e -> {
            LOGGER.error("{}  buildMerged can not create merged channel:\n{}", reqCtx, ErrorUtils.traceString(e));
            return new RequestException(reqCtx, "request error");
        });
    }

    static Mono<SubStream> springWebClientRequest(ReqCtx reqCtx, String localURL, String channelName, String js) {
        Flux<DataBuffer> fl = WebClient.builder()
        .baseUrl(localURL)
        .build()
        .post()
        .contentType(MediaType.APPLICATION_JSON)
        .accept(MediaType.APPLICATION_OCTET_STREAM)
        .header("connection", "close")
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
        ret.fl = fl
        .map(k -> BufCont.fromBuffer(k, BufCont.Mark.SprWebCl));
        return Mono.just(ret);
    }

    static Mono<SubStream> springWebClientRequestFake(ReqCtx reqCtx, String localURL, String channelName, String js) {
        SubStream ret = new SubStream();
        ret.reqId = "dummy";
        ret.fl = Flux.generate(() -> GenSt.create(reqCtx.bufCtx), GenSt::next, GenSt::release);
        return Mono.just(ret);
    }

    static Mono<SubHeadRes> subHeadRes(ReqCtx reqCtx, Node node, String channelName, Flux<BufCont> fl) {
        AtomicLong tsL = new AtomicLong(System.nanoTime());
        AtomicInteger prest = new AtomicInteger();
        AtomicInteger jslen = new AtomicInteger();
        AtomicReference<ChannelConfigEntry> channelConfigEntryRef = new AtomicReference<>();
        ByteBuffer bbPre = ByteBuffer.allocate(64);
        ByteBuffer bbJs = ByteBuffer.allocate(1024);
        return fl
        .doOnNext(q -> q.appendMark(BufCont.Mark.SubTools1))
        .limitRate(1)
        .doOnNext(k -> {
            long ts = System.nanoTime();
            long dt = ts - tsL.get();
            if (dt > 1000L * 1000 * 1000 * 20) {
                LOGGER.warn("{}  WebClient Response  long delay {}  {}  {}", reqCtx, dt, node.host, channelName);
            }
            tsL.set(ts);
        })
        .doOnSubscribe(s -> {
            LOGGER.debug("{}  WebClient Response  {}  {}  {}  SUBSCRIBE", reqCtx, channelName, node.host, node.port);
        })
        .concatMap(bc -> {
            DataBuffer buf = bc.bufferRef();
            while (prest.get() < 2) {
                if (prest.get() == 0) {
                    if (bbPre.position() < 12) {
                        int move = Math.min(12 - bbPre.position(), buf.readableByteCount());
                        for (int i2 = 0; i2 < move; i2 += 1) {
                            bbPre.put(buf.read());
                        }
                    }
                    if (bbPre.position() > 12) {
                        throw new RuntimeException("logic");
                    }
                    if (bbPre.position() == 12) {
                        prest.set(1);
                        bbPre.flip();
                        CharBuffer pre = StandardCharsets.UTF_8.decode(bbPre.slice().limit(8));
                        if (!pre.toString().equals("CHANHEAD")) {
                            LOGGER.error("{}  missing tag CHANHEAD", reqCtx);
                        }
                        bbPre.position(bbPre.position() + 8);
                        jslen.set(bbPre.getInt());
                        LOGGER.debug("Pre-Header:   {}  {}", pre, jslen.get());
                    }
                }
                else if (prest.get() == 1) {
                    if (bbJs.position() < jslen.get()) {
                        int move = Math.min(jslen.get() - bbJs.position(), buf.readableByteCount());
                        for (int i2 = 0; i2 < move; i2 += 1) {
                            bbJs.put(buf.read());
                        }
                    }
                    if (bbJs.position() == jslen.get()) {
                        prest.set(2);
                        bbJs.flip();
                        String s = StandardCharsets.UTF_8.decode(bbJs).toString();
                        LOGGER.debug("got js  {}", s);
                        try {
                            ChannelConfigEntry e = new ObjectMapper().readValue(s, ChannelConfigEntry.class);
                            channelConfigEntryRef.set(e);
                        }
                        catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
                if (buf.readableByteCount() <= 0) {
                    break;
                }
            }
            if (buf.readableByteCount() > 0) {
                return Mono.just(bc);
            }
            else {
                bc.close();
                return Mono.empty();
            }
        }, 0)
        .doOnNext(k -> k.appendMark(BufCont.Mark.SubToolsBuf))
        .doOnNext(k -> LOGGER.trace("{}  collect next buffer for list  host {}  rb {}", reqCtx, node.host, k.readableByteCount()))
        .collectList()
        .map(k -> {
            //LOGGER.info("{}  map to SubHeadRes {}  {}", reqCtx, k.size(), channelConfigEntryRef.get().toString());
            SubHeadRes ret = new SubHeadRes();
            ret.channelConfigEntry = channelConfigEntryRef.get();
            ret.bufs = k;
            return ret;
        });
    }

}
