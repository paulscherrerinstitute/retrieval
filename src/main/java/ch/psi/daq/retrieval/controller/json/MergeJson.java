package ch.psi.daq.retrieval.controller.json;

import ch.psi.daq.retrieval.QueryParams;
import ch.psi.daq.retrieval.reqctx.ReqCtx;
import ch.psi.daq.retrieval.bytes.BufCont;
import ch.psi.daq.retrieval.bytes.BufContOutputStream;
import ch.psi.daq.retrieval.controller.QueryData;
import ch.psi.daq.retrieval.controller.mergefunc.MergeFunction;
import ch.psi.daq.retrieval.controller.mergefunc.MergeFunctionDefault;
import ch.psi.daq.retrieval.eventmap.value.TransformSupplier;
import ch.psi.daq.retrieval.merger.MergerSupport;
import ch.psi.daq.retrieval.pod.api1.Query;
import ch.psi.daq.retrieval.status.Error;
import ch.psi.daq.retrieval.subnodes.SubTools;
import ch.psi.daq.retrieval.throttle.Throttle;
import ch.qos.logback.classic.Logger;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class MergeJson {
    static Logger LOGGER = (Logger) LoggerFactory.getLogger("CtrlMergJson");

    public Mono<ResponseEntity<Flux<DataBuffer>>> queryMergedJson(ReqCtx reqCtx, QueryData queryData, Mono<Query> queryMono) {
        LOGGER.info("{}  queryMergedJson", reqCtx);
        queryData.requestStatusBoard.requestBegin(reqCtx);
        return queryMono
        .doOnError(x -> LOGGER.error("{}  can not parse request", reqCtx))
        .map(query -> {
            QueryParams qp = QueryParams.fromQuery(queryData.conf, query, reqCtx.bufCtx);
            LOGGER.info("{}  queryMergedJson  {}  {}  {}  {}", reqCtx, qp.begin, qp.end, qp.channels, qp.splits);
            AtomicLong nbytes = new AtomicLong();
            TransformSupplier<JsonNode> valuemapSup = new TransSupJson(reqCtx, qp);
            MergeFunction mergeFunction = new MergeFunctionDefault();
            BufContOutputStream bcosInit = new BufContOutputStream(reqCtx.bufCtx, BufCont.Mark.__NOMARK);
            AtomicReference<BufContOutputStream> bcosRef = new AtomicReference<>(bcosInit);
            JsonFactory jfac = new JsonFactory();
            AtomicReference<JsonGenerator> jgen = new AtomicReference<>();
            try {
                jgen.set(jfac.createGenerator(bcosRef.get()));
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            ObjectMapper om = new ObjectMapper();
            try {
                JsonGenerator jg = jgen.get();
                jg.writeStartObject();
                jg.writeArrayFieldStart("channels");
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            return Flux.fromIterable(qp.channels)
            .concatMap(channel -> {
                try {
                    JsonGenerator jg = jgen.get();
                    jg.writeStartObject();
                    jg.writeStringField("name", channel.name);
                    jg.writeArrayFieldStart("bins");
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
                return SubTools.buildMergedChannel(reqCtx, channel, qp, qp.begin, qp.end, qp.trailingEventsMax, mergeFunction, queryData.requestStatusBoard(), queryData.conf)
                .flatMapMany(mc -> {
                    return mc.fl
                    .transform(fl -> MergerSupport.flattenSlices(fl, qp.endNanos, channel.name, qp, reqCtx.bufCtx))
                    .doOnNext(bc -> {
                        long h = nbytes.addAndGet(bc.readableByteCount());
                        if (query.errorAfterBytes > 0 && h > query.errorAfterBytes) {
                            throw new RuntimeException("errorAfterBytes");
                        }
                    })
                    .doOnError(e -> {
                        LOGGER.error("{}  queryMergedJson ERROR  {}", reqCtx, e.toString());
                        queryData.requestStatusBoard.getOrCreate(reqCtx).addError(new Error(e));
                    })
                    .transform(fl -> valuemapSup.trans(fl, channel, mc.configEntry))
                    .map(k -> {
                        try {
                            JsonGenerator jg = jgen.get();
                            om.writeTree(jg, k);
                            BufContOutputStream bcos = bcosRef.get();
                            if (bcos.queuedCount() > 0) {
                                List<BufCont> q = bcos.takeAndStart();
                                return q;
                            }
                            else {
                                return List.<BufCont>of();
                            }
                        }
                        catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
                })
                .doOnComplete(() -> {
                    try {
                        JsonGenerator jg = jgen.get();
                        jg.writeEndArray();
                        jg.writeEndObject();
                    }
                    catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            }, 0)
            .concatWith(Mono.defer(() -> {
                try {
                    JsonGenerator jg = jgen.get();
                    jg.writeEndArray();
                    jg.writeEndObject();
                    jg.close();
                    return Mono.just(bcosRef.get().take());
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }))
            .concatMapIterable(Function.identity())
            .doFinally(k -> {
                bcosRef.get().release();
                bcosRef.set(null);
            })
            .map(BufCont::takeBuf)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .doOnNext(k -> {
                int rb = k.readableByteCount();
                QueryData.totalBytesEmitted.getAndAdd(rb);
                reqCtx.addBodyLen(rb);
            })
            .transform(fl -> Throttle.throttle(fl, qp.throttleRate, qp.throttleSteps, qp.throttleInterval, qp.throttleOverslack))
            .transform(queryData.statusPing(reqCtx))
            .transform(QueryData.doDiscard("queryMergedJson final"))
            .doOnDiscard(Object.class, QueryData::doDiscardFinal)
            .doFinally(sig -> queryData.requestStatusBoard.bodyEmitted(reqCtx, sig));
        })
        .map(fl -> {
            return ResponseEntity.ok()
            .header(QueryData.HEADER_X_CANONICALHOSTNAME, queryData.conf.canonicalHostname)
            .header(QueryData.HEADER_X_DAQBUFFER_REQUEST_ID, reqCtx.reqId)
            .contentType(MediaType.APPLICATION_JSON)
            .body(fl);
        });
    }

}
