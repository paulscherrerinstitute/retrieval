package ch.psi.daq.retrieval.controller.json;

import ch.psi.daq.retrieval.reqctx.BufCtx;
import ch.psi.daq.retrieval.QueryParams;
import ch.psi.daq.retrieval.reqctx.ReqCtx;
import ch.psi.daq.retrieval.bytes.BufCont;
import ch.psi.daq.retrieval.config.ChannelConfigEntry;
import ch.psi.daq.retrieval.eventmap.agg.Agg;
import ch.psi.daq.retrieval.eventmap.agg.BinTrack;
import ch.psi.daq.retrieval.eventmap.basic.Basic;
import ch.psi.daq.retrieval.eventmap.basic.BasicParams;
import ch.psi.daq.retrieval.eventmap.unpack.scalarnum.IntAgg;
import ch.psi.daq.retrieval.eventmap.unpack.scalarnum.Num;
import ch.psi.daq.retrieval.eventmap.unpack.scalarnum.nums.F32;
import ch.psi.daq.retrieval.eventmap.unpack.scalarnum.nums.I64;
import ch.psi.daq.retrieval.eventmap.unpack.scalarnum.nums.U64;
import ch.psi.daq.retrieval.eventmap.unpack.scalarnum.UnpackScalarNum;
import ch.psi.daq.retrieval.eventmap.value.TransformSupplier;
import ch.psi.daq.retrieval.pod.api1.Channel;
import ch.qos.logback.classic.Logger;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

class TransSupJson implements TransformSupplier<JsonNode> {
    static final Logger LOGGER = (Logger) LoggerFactory.getLogger(TransSupJson.class.getSimpleName());

    TransSupJson(ReqCtx reqCtx, QueryParams qp) {
        this.reqCtx = reqCtx;
        this.qp = qp;
        bufCtx = reqCtx.bufCtx;
    }

    public Flux<JsonNode> trans(Flux<BufCont> fl, Channel channel, ChannelConfigEntry configEntry) {
        // TODO cover strings
        // TODO cover bool
        // TODO cover char
        if (!configEntry.isArray && !configEntry.isCompressed) {
            int dt = configEntry.dtType;
            // TODO also discriminate on endianness
            if (dt >= 2 && dt <= 5 || dt >= 7 && dt <= 12) {
                return transInt(fl, channel.name, configEntry);
            }
            else {
                return null;
            }
        }
        else {
            return null;
        }
    }

    Flux<JsonNode> transInt(Flux<BufCont> fl, String channelName, ChannelConfigEntry configEntry) {
        switch (configEntry.dtType) {
            // TODO add more variants
            case 9:
                LOGGER.info("transInt  I64");
                return transIntNum(fl, channelName, configEntry, new I64());
            case 10:
                LOGGER.info("transInt  U64");
                return transIntNum(fl, channelName, configEntry, new U64());
            case 11:
                LOGGER.info("transInt  F32");
                return transIntNum(fl, channelName, configEntry, new F32());
            default:
                return null;
        }
    }

    <T extends Num<T>> Flux<JsonNode> transIntNum(Flux<BufCont> fl, String channelName, ChannelConfigEntry configEntry, T num) {
        ObjectMapper om = new ObjectMapper();
        BasicParams params = new BasicParams(bufCtx, qp.endNanos, String.format("%s-%s", reqCtx.reqId, channelName));
        List<IntAgg<T>> aggs = new ArrayList<>();
        if (qp.aggregation != null) {
            qp.aggregation.operators.forEach(op -> {
                if (op.equals("minmaxavg")) {
                    aggs.add(num.createMinMaxAvg());
                }
            });
        }
        // TODO activate binning only if requested, but limit in all cases
        long bw;
        int n;
        if (qp.aggregation == null) {
            n = 20;
            bw = (qp.endNanos - qp.begNanos) / n;
        }
        else {
            n = qp.aggregation.nrOfBins;
            bw = (qp.endNanos - qp.begNanos) / n;
        }
        BinTrack binTrack = new BinTrack(0, qp.begNanos, 0, qp.endNanos, 0, n);
        return Basic.trans(fl, params)
        .transform(fl2 -> UnpackScalarNum.trans(fl2, num))
        .map(k -> {
            LOGGER.info("incoming {} events", k.tss().size());
            List<JsonNode> ret = new ArrayList<>();
            {
                int i1 = 0;
                int i2 = 0;
                int nn = k.tss().size();
                // loop until there is more incoming data to handle
                while (i1 < nn && !binTrack.term()) {
                    {
                        // At this point the batch is empty.
                        // If our candidate front at this point is before the current bin then we've lost
                        // because we can not rewind BinTrack.
                        long ts = k.tss().get(i1);
                        long pulse = k.pulses().get(i1);
                        if (binTrack.isBefore(ts, pulse)) {
                            throw new RuntimeException("logic");
                        }
                    }
                    {
                        long ts = k.tss().get(i1);
                        long pulse = k.pulses().get(i1);
                        while (binTrack.advance(ts, pulse)) {
                            if (binTrack.modeTs()) {
                                LOGGER.info("emit inloop  {}  {}", binTrack.lastBegin() / 1000 / 1000, binTrack.lastEnd() / 1000 / 1000);
                                AggBinTs ab = new AggBinTs(binTrack.lastBegin(), binTrack.lastEnd());
                                ab.results = aggs.stream().map(Agg::toJsonNode).collect(Collectors.toList());
                                ret.add(om.valueToTree(ab));
                            }
                            else if (binTrack.modePulse()) {
                                ret.add(om.valueToTree("n/a"));
                            }
                            aggs.forEach(Agg::clear);
                            if (binTrack.term()) {
                                break;
                            }
                        }
                    }
                    while (i2 < nn) {
                        // Get the ts from the front of the current candidate batch
                        long ts = k.tss().get(i2);
                        long pulse = k.pulses().get(i2);
                        if (binTrack.isAfter(ts, pulse)) {
                            break;
                        }
                        i2 += 1;
                    }
                    if (i2 <= i1) {
                        throw new RuntimeException("logic");
                    }
                    LOGGER.trace("consider  ts  {}  {}", k.tss().get(i1) / 1000 / 1000, k.tss().get(i2-1) / 1000 / 1000);
                    for (IntAgg<T> q : aggs) {
                        q.consider(k, i1, i2);
                    }
                    i1 = i2;
                }
            }
            return ret;
        })

        .concatWith(Mono.defer(() -> {
            List<JsonNode> ret = new ArrayList<>();
            while (!binTrack.term()) {
                LOGGER.info("emit afterwards  {}  {}", binTrack.currentBegin() / 1000 / 1000, binTrack.currentEnd() / 1000 / 1000);
                AggBinTs ab = new AggBinTs(binTrack.currentBegin(), binTrack.currentEnd());
                ab.results = aggs.stream().map(Agg::toJsonNode).collect(Collectors.toList());
                ret.add(om.valueToTree(ab));
                aggs.forEach(Agg::clear);
                binTrack.gotoNextBin();
            }
            return Mono.just(ret);
        }))
        .concatMapIterable(Function.identity())

        /*
        // map to final
        .concatMapIterable(k -> {
            return k.stream().map(q -> {
                ArrayNode an = jnfac.arrayNode(q.size());
                for (JsonNode nn : q) {
                    an.add(nn);
                }
                return an;
            }).collect(Collectors.toList());
        })
        */

        /*
        .map(k -> {
            try {
                String s = om.writeValueAsString(k);
                return BufCont.fromString(s);
            }
            catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        })
        */


        /*
        .then(Mono.defer(() -> {
            ObjectNode aggResults = jnfac.objectNode();
            StringBuilder sb = new StringBuilder();
            aggs.forEach(k -> {
                sb.append(k.finalString()).append('\n');
                aggResults.set(k.name(), k.toJsonNode());
            });
            //return Mono.just(BufCont.fromString(sb));
            try {
                String s = om.writeValueAsString(aggResults);
                return Mono.just(BufCont.fromString(s));
            }
            catch (JsonProcessingException e) {
                return Mono.error(e);
            }
        }))
        .flux()
        */
        ;
    }

    Class<? extends Num<?>> __dtClass(int ty) {
        switch (ty) {
            case 10:
                return U64.class;
            default:
                return null;
        }
    }

    <T extends Num<T>> Class<T> dtClass(int ty) {
        switch (ty) {
            case 10:
                // TODO why impossible?
                //return U64.class;
            default:
                return null;
        }
    }

    Supplier<? extends Num<?>> dtNumSup(int ty) {
        switch (ty) {
            case 10:
                return U64::new;
            default:
                return null;
        }
    }

    final ReqCtx reqCtx;
    final QueryParams qp;
    final BufCtx bufCtx;

}
