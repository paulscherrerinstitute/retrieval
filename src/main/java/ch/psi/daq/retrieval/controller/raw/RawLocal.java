package ch.psi.daq.retrieval.controller.raw;

import ch.psi.daq.retrieval.reqctx.BufCtx;
import ch.psi.daq.retrieval.utils.ChannelEventStream;
import ch.psi.daq.retrieval.QueryParams;
import ch.psi.daq.retrieval.reqctx.ReqCtx;
import ch.psi.daq.retrieval.bytes.BufCont;
import ch.psi.daq.retrieval.config.ChannelConfigEntry;
import ch.psi.daq.retrieval.controller.QueryData;
import ch.psi.daq.retrieval.eventmap.ts.EventBlobToV1MapTs;
import ch.psi.daq.retrieval.eventmap.ts.MapTsItemVec;
import ch.psi.daq.retrieval.pod.api1.Query;
import ch.psi.daq.retrieval.throttle.Throttle;
import ch.qos.logback.classic.Logger;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

public class RawLocal {
    static Logger LOGGER = (Logger) LoggerFactory.getLogger(RawLocal.class.getSimpleName());

    public Mono<ResponseEntity<Flux<DataBuffer>>> rawLocal(ReqCtx reqCtx, QueryData queryData, ServerWebExchange exchange, Mono<Query> queryMono) {
        return queryMono
        .flatMap(query -> {
            return rawLocalInner(reqCtx, queryData, Mono.just(query))
            .flatMap(fl -> {
                return queryData.logResponse(reqCtx, "rawLocal", Mono.just(fl));
            });
        });
    }

    static Mono<BufCont> streamConfigHeader(ChannelConfigEntry channelConfigEntry, BufCtx bufCtx) {
        try {
            String js = new ObjectMapper().writeValueAsString(channelConfigEntry);
            byte[] jsBytes = js.getBytes(StandardCharsets.UTF_8);
            ByteBuffer bb = ByteBuffer.allocate(32);
            BufCont bc = BufCont.allocate(bufCtx.bufFac, bufCtx.bufferSize, BufCont.Mark.PreChanConf);
            DataBuffer b = bc.bufferRef();
            b.write("CHANHEAD", StandardCharsets.UTF_8);
            bb.clear();
            bb.putInt(jsBytes.length);
            bb.flip();
            b.write(bb);
            b.write(jsBytes);
            return Mono.just(bc);
        }
        catch (Exception e) {
            return Mono.error(e);
        }
    }

    public Mono<Flux<DataBuffer>> rawLocalInner(ReqCtx reqCtx, QueryData queryData, Mono<Query> queryMono) {
        return queryMono
        .doOnError(x -> LOGGER.error("{}  can not parse request", reqCtx))
        .map(query -> {
            queryData.requestStatusBoard.requestSubBegin(reqCtx, query.mainReqId);
            QueryParams qp = QueryParams.fromQuery(queryData.conf, query, reqCtx.bufCtx);
            LOGGER.debug("{}  rawLocal  {}  {}  {}", reqCtx, qp.begin, qp.end, qp.channels);
            if (qp.channels.size() != 1) {
                LOGGER.error("{}  rawLocal  requested more than one channel  {}  {}  {}", reqCtx, qp.begin, qp.end, qp.channels);
                throw new RuntimeException("logic");
            }
            if (qp.splits.size() != 1) {
                LOGGER.error("{}  rawLocal  requested more than one split  {}  {}  {}", reqCtx, qp.begin, qp.end, qp.splits);
                throw new RuntimeException("logic");
            }
            int split = qp.splits.get(0);
            final String channelName = qp.channels.get(0).name;
            return ChannelEventStream.channelDataFluxes(reqCtx, qp, queryData.baseDirFinder, qp.begin, qp.end, Duration.ZERO, channelName, split)
            .flatMapMany(res -> {
                AtomicLong bytesEmit = new AtomicLong();
                return res.fl
                .transform(fl2 -> EventBlobToV1MapTs.trans(reqCtx, fl2, String.format("[rawLocalInner  split %d  %s]", split, channelName), channelName, qp.endNanos, qp.trailingEventsMax, reqCtx.bufCtx))
                .doOnNext(kk -> kk.markWith(BufCont.Mark.qdrl1))
                .concatMapIterable(MapTsItemVec::takeBuffers, qp.rawItemVecPrefetch + 1)
                .doOnNext(kk -> kk.appendMark(BufCont.Mark.qdrl2))
                .transform(QueryData.doDiscard("rawLocalqdrl2"))
                .takeWhile(buf -> {
                    if (qp.limitBytesPerChannel == 0 || bytesEmit.get() < qp.limitBytesPerChannel) {
                        return true;
                    }
                    else {
                        buf.close();
                        return false;
                    }
                })
                .doOnNext(k -> bytesEmit.getAndAdd(k.readableByteCount()))
                .doOnNext(kk -> kk.appendMark(BufCont.Mark.qdrl3))
                .transform(QueryData.doDiscard("rawLocalqdrl3"))
                .startWith(streamConfigHeader(res.channelConfigEntry, reqCtx.bufCtx))
                .doOnNext(kk -> kk.appendMark(BufCont.Mark.qdrl4))
                .transform(QueryData.doDiscard("rawLocalqdrl4"))
                ;
            })
            .transform(fl -> Throttle.throttleBufCont(fl, qp.throttleRate, qp.throttleSteps, qp.throttleInterval, qp.throttleOverslack))
            .transform(queryData.statusPing(reqCtx))
            .doOnNext(kk -> kk.appendMark(BufCont.Mark.qdrl7))
            .transform(QueryData.doDiscard("rawLocalqdrl7"))
            .map(k -> {
                if (k.closed()) {
                    QueryData.rawLocalClosedBufCont.getAndAdd(1);
                    LOGGER.error("{}  rawLocalInner  closed BufCont while attempt to takeBuf", reqCtx);
                    return Optional.<DataBuffer>empty();
                }
                else {
                    return k.takeBuf();
                }
            })
            .filter(Optional::isPresent)
            .map(Optional::get)
            .doOnCancel(() -> {
                LOGGER.debug("{}  rawLocal sig  CANCEL    {}", reqCtx, channelName);
            })
            .doOnError(e -> {
                LOGGER.error("{}  rawLocal  ERROR  {}  {}", reqCtx, channelName, e.toString());
                queryData.requestStatusBoard.requestErrorChannelName(reqCtx, channelName, e);
            })
            .doOnNext(buf -> {
                int rb = buf.readableByteCount();
                QueryData.rawLocalBytesEmitted.getAndAdd(rb);
                reqCtx.addBodyLen(rb);
            })
            .transform(QueryData.doDiscard("rawLocalEnd"))
            .doFinally(sig -> queryData.requestStatusBoard.bodyEmitted(reqCtx, sig));
        })
        .doOnError(e -> {
            LOGGER.error("{}  rawLocal  {}", reqCtx, e.toString());
            queryData.requestStatusBoard.requestError(reqCtx, e);
        });
    }

}
