package ch.psi.daq.retrieval.merger;

import ch.psi.daq.retrieval.pod.api1.Channel;
import ch.psi.daq.retrieval.reqctx.BufCtx;
import ch.psi.daq.retrieval.QueryParams;
import ch.psi.daq.retrieval.bytes.BufCont;
import ch.psi.daq.retrieval.bytes.Output;
import ch.psi.daq.retrieval.controller.QueryData;
import ch.psi.daq.retrieval.eventmap.ts.MapTsItemVec;
import ch.psi.daq.retrieval.eventmap.ts.MapTsToken;
import ch.psi.daq.retrieval.reqctx.ReqCtx;
import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class MergerSupport {
    static final Logger LOGGER = (Logger) LoggerFactory.getLogger(MergerSupport.class.getSimpleName());

    public static Flux<MapTsToken> mergeItemVecFluxes(List<Flux<MapTsItemVec>> fls, ReqCtx reqCtx, Channel channel, QueryParams qp) {
        List<Flux<MapTsToken>> fls2 = fls.stream().map(k -> {
            return k.doOnNext(q -> q.markWith(BufCont.Mark.mergeItemVecFluxesA))
            .concatMap(q -> q.intoFlux(88), 0)
            .doOnNext(q -> q.markWith(BufCont.Mark.mergeItemVecFluxesB));
        })
        .collect(Collectors.toList());
        return mergeTsTokenFluxes(fls2, reqCtx, channel, qp);
    }

    public static Flux<MapTsToken> mergeTsTokenFluxes(List<Flux<MapTsToken>> fls, ReqCtx reqCtx, Channel channel, QueryParams qp) {
        List<Flux<MapTsToken>> ret = new ArrayList<>();
        for (Flux<MapTsToken> fl : fls) {
            Flux<MapTsToken> fl2 = fl
            .doOnNext(k -> k.appendMark(BufCont.Mark.msiv1))
            .transform(QueryData.doDiscard("mergeItemVecFluxesB"));
            ret.add(fl2);
        }
        return Flux.from(Merger.merge(new ArrayList<>(ret), qp.endNanos, String.format("%s %s", reqCtx, channel.name)));
    }

    static class Flatten {
        Flatten(String channelName, QueryParams qp, BufCtx bufCtx) {
            this.channelName = channelName;
            this.qp = qp;
            this.bufCtx = bufCtx;
        }
        List<BufCont> next(MapTsToken tok) {
            if (released) {
                LOGGER.error("Flatten  next  released");
                throw new RuntimeException("logic");
            }
            //LOGGER.info("Flatten  sees {} {}", tok.ts, tok.ty.toString());
            if (tok.ty == MapTsItemVec.Ty.OPEN) {
                if (tyLast != MapTsItemVec.Ty.CLOSE && tyLast != MapTsItemVec.Ty.FULL) {
                    throw new RuntimeException("OPEN after " + tyLast.toString());
                }
            }
            else if (tok.ty == MapTsItemVec.Ty.CLOSE) {
                if (tyLast != MapTsItemVec.Ty.OPEN && tyLast != MapTsItemVec.Ty.MIDDLE) {
                    throw new RuntimeException("CLOSE after " + tyLast.toString());
                }
            }
            else if (tok.ty == MapTsItemVec.Ty.MIDDLE) {
                if (tyLast != MapTsItemVec.Ty.OPEN && tyLast != MapTsItemVec.Ty.MIDDLE) {
                    throw new RuntimeException("MIDDLE after " + tyLast.toString());
                }
            }
            else if (tok.ty == MapTsItemVec.Ty.FULL) {
                if (tyLast != MapTsItemVec.Ty.CLOSE && tyLast != MapTsItemVec.Ty.FULL) {
                    throw new RuntimeException("FULL after " + tyLast.toString());
                }
            }
            if (out == null) {
                out = new Output(bufCtx.bufFac, bufCtx.bufferSize, BufCont.Mark.Flatten);
            }
            DataBuffer buf = tok.bufcont.bufferRef();
            out.ensureWritable(buf.readableByteCount());
            out.transfer(buf.slice(tok.pos, tok.len), tok.len);
            totalBytes.getAndAdd(tok.len);
            tok.release();
            tyLast = tok.ty;
            if (out.queuedCount() > 0) {
                flattenEmit.getAndIncrement();
                return out.take();
            }
            else {
                flattenKeep.getAndIncrement();
                return List.of();
            }
        }
        void release() {
            if (out != null) {
                out.release();
                out = null;
            }
            released = true;
        }
        List<BufCont> last() {
            if (out != null) {
                List<BufCont> ret = out.take();
                return ret;
            }
            else {
                return List.of();
            }
        }
        final AtomicLong totalBytes = new AtomicLong();
        final String channelName;
        final QueryParams qp;
        final BufCtx bufCtx;
        boolean released;
        Output out;
        MapTsItemVec.Ty tyLast = MapTsItemVec.Ty.CLOSE;
    }

    public static Flux<CompleteEvent> eventsFromTokens(Flux<MapTsToken> fl) {
        return fl.bufferUntil(k -> {
            return k.ty == MapTsItemVec.Ty.FULL || k.ty == MapTsItemVec.Ty.CLOSE;
        })
        .filter(k -> {
            if (k.isEmpty()) {
                bufferUntilEmitEmpty.getAndIncrement();
                return false;
            }
            else {
                return true;
            }
        })
        .map(k -> new CompleteEvent(k.get(0).ts, k.get(0).pulse, k))
        .filter(k -> {
            if (k.isValid()) {
                return true;
            }
            else {
                bufferUntilEmitInvalid.getAndIncrement();
                k.releaseFinite();
                return false;
            }
        });
    }

    public static Flux<BufCont> flattenSlices(Flux<MapTsToken> fls, long endNanos, String channelName, QueryParams qp, BufCtx bufCtx) {
        Flatten flatten = new Flatten(channelName, qp, bufCtx);
        Predicate<MapTsToken> tw = tok -> {
            if (tok.ts < endNanos) {
                return true;
            }
            else {
                tok.release();
                return false;
            }
        };
        return fls
        .takeWhile(tw)
        .transform(QueryData.doDiscard("flattenSlices"))
        .concatMapIterable(flatten::next, qp.flattenSlicesPrefetch + 1)
        .concatWith(Flux.defer(() -> {
            return Flux.fromIterable(flatten.last());
        }))
        .doFinally(sig -> flatten.release())
        .doOnNext(k -> k.appendMark(BufCont.Mark.FlattenPassOn));
    }

    public static class Stats {
        public long bufferUntilEmitEmpty;
        public long bufferUntilEmitInvalid;
        public long flattenEmit;
        public long flattenKeep;
        public Stats() {
            bufferUntilEmitEmpty = MergerSupport.bufferUntilEmitEmpty.get();
            bufferUntilEmitInvalid = MergerSupport.bufferUntilEmitInvalid.get();
            flattenEmit = MergerSupport.flattenEmit.get();
            flattenKeep = MergerSupport.flattenKeep.get();
        }
    }

    static final AtomicLong bufferUntilEmitEmpty = new AtomicLong();
    static final AtomicLong bufferUntilEmitInvalid = new AtomicLong();
    static final AtomicLong flattenEmit = new AtomicLong();
    static final AtomicLong flattenKeep = new AtomicLong();

}
