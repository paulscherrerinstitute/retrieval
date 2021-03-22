package ch.psi.daq.retrieval.controller.octets;

import ch.psi.daq.retrieval.QueryParams;
import ch.psi.daq.retrieval.bytes.BufCont;
import ch.psi.daq.retrieval.controller.QueryData;
import ch.psi.daq.retrieval.eventmap.ts.EventBlobToV1MapTs;
import ch.psi.daq.retrieval.eventmap.value.EventBlobMapResult;
import ch.psi.daq.retrieval.eventmap.value.EventBlobToV1Map;
import ch.psi.daq.retrieval.merger.MergerSupport;
import ch.psi.daq.retrieval.pod.api1.Query;
import ch.psi.daq.retrieval.reqctx.ReqCtx;
import ch.psi.daq.retrieval.throttle.Throttle;
import ch.psi.daq.retrieval.utils.ChannelEventStream;
import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class MergedOctetsLocal {
    static Logger LOGGER = (Logger) LoggerFactory.getLogger(MergedOctetsLocal.class.getSimpleName());

    public Mono<ResponseEntity<Flux<DataBuffer>>> queryMergedOctetsLocal(ReqCtx reqCtx, QueryData queryData, Mono<Query> queryMono) {
        LOGGER.info("{}  queryMergedOctetsLocal", reqCtx);
        queryData.requestStatusBoard.requestBegin(reqCtx);
        return queryMono
        .doOnError(x -> LOGGER.error("{}  can not parse request", reqCtx))
        .map(query -> {
            QueryParams qp = QueryParams.fromQuery(queryData.conf, query, reqCtx.bufCtx);
            LOGGER.info("{}  queryMergedOctetsLocal  {}  {}  {}  {}", reqCtx, qp.begin, qp.end, qp.channels, qp.splits);
            long endNanos = qp.endNanos;
            List<Integer> splits = queryData.conf.splits.stream()
            .filter(split -> qp.splits == null || qp.splits.isEmpty() || qp.splits.contains(split))
            .collect(Collectors.toList());
            return Flux.fromIterable(query.channels.channels)
            .concatMap(channel -> {
                return Flux.fromIterable(splits)
                .map(split -> {
                    return ChannelEventStream.channelDataFluxes(reqCtx, qp, queryData.baseDirFinder, qp.begin, qp.end, Duration.ZERO, channel.name, split)
                    .flatMapMany(res -> {
                        return res.fl
                        .transform(fl2 -> EventBlobToV1MapTs.trans(reqCtx, fl2, String.format("[queryMergedOctetsLocal %s]", channel.name), channel.name, endNanos, 1, reqCtx.bufCtx));
                    });
                })
                .collectList()
                .flatMapMany(fll -> {
                    return MergerSupport.mergeItemVecFluxes(fll, reqCtx, channel, qp)
                    .doOnNext(q -> q.markWith(BufCont.Mark.OcLo1))
                    .transform(fl -> MergerSupport.flattenSlices(fl, endNanos, channel.name, qp, reqCtx.bufCtx))
                    .doOnNext(q -> q.appendMark(BufCont.Mark.OcLo2))
                    .transform(fl -> EventBlobToV1Map.trans(reqCtx, fl, channel.name, endNanos, reqCtx.bufCtx, qp))
                    .doOnNext(q -> q.appendMark(BufCont.Mark.OcLo3))
                    .concatMapIterable(EventBlobMapResult::takeBufCont, 1)
                    .transform(QueryData.doDiscard("queryMergedOctetsLocalChannelEnd"))
                    .transform(BufAcc::trans)
                    ;
                    //MergerSupport.mergeTsTokenFluxes(fll, qp);
                    //return MergerSupport.flattenSlices(MergerSupport.mergeItemVecFluxes(lfl, qp), qp.endNanos, channelName, qp, bufCtx);
                    //return pipeThroughMerger(reqCtx, fll, channel.name, qp, reqCtx.bufCtx)
                    //.transform(flbuf -> EventBlobToV1Map.trans(reqCtx, flbuf, channel.name, endNanos, reqCtx.bufCtx, qp))
                });
            }, 0)
            .map(k -> {
                if (k.closed()) {
                    LOGGER.error("{}  queryMergedOctetsLocal  closed BufCont while attempt to takeBuf", reqCtx);
                    return Optional.<DataBuffer>empty();
                }
                else {
                    return k.takeBuf();
                }
            })
            .filter(Optional::isPresent)
            .map(Optional::get)
            .doOnNext(k -> {
                int rb = k.readableByteCount();
                QueryData.totalBytesEmitted.getAndAdd(rb);
                reqCtx.addBodyLen(rb);
            })
            .transform(fl -> Throttle.throttle(fl, qp.throttleRate, qp.throttleSteps, qp.throttleInterval, qp.throttleOverslack))


            // TODO enable again:
            //.transform(queryData.statusPing(reqCtx))

            //.doOnNext(k -> LOGGER.info("{}  next  rb {}  rp {}  wp {}  cap {}", reqCtx, k.readableByteCount(), k.readPosition(), k.writePosition(), k.capacity()))
            //.bufferTimeout(16, Duration.ofMillis(2000))
            //.delayElements(Duration.ofMillis(10))
            //.doOnNext(k -> LOGGER.info("{}  queryMergedOctetsLocal  buf out  size {}", reqCtx, k.size()))
            //.concatMapIterable(Function.identity(), 1)
            .transform(QueryData.doDiscard("queryMergedOctetsLocalEnd"))
            .doFinally(sig -> queryData.requestStatusBoard.bodyEmitted(reqCtx, sig));
        })
        .map(fl -> {
            return ResponseEntity.ok()
            .header(QueryData.HEADER_X_CANONICALHOSTNAME, queryData.conf.canonicalHostname)
            .header(QueryData.HEADER_X_DAQBUFFER_REQUEST_ID, reqCtx.reqId)
            .contentType(MediaType.APPLICATION_OCTET_STREAM)
            .body(fl);
        });
    }

}
