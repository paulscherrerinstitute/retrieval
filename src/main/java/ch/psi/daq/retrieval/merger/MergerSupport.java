package ch.psi.daq.retrieval.merger;

import ch.psi.daq.retrieval.BufCtx;
import ch.psi.daq.retrieval.QueryParams;
import ch.psi.daq.retrieval.bytes.BufCont;
import ch.psi.daq.retrieval.bytes.Output;
import ch.psi.daq.retrieval.controller.QueryData;
import ch.psi.daq.retrieval.eventmap.ts.MapTsItemVec;
import ch.psi.daq.retrieval.eventmap.ts.MapTsToken;
import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class MergerSupport {
    static final Logger LOGGER = (Logger) LoggerFactory.getLogger(MergerSupport.class.getSimpleName());

    public static Flux<MapTsToken> mergeItemVecFluxes(List<Flux<MapTsItemVec>> fls, QueryParams qp) {
        List<Flux<MapTsToken>> ret = new ArrayList<>();
        int i1 = 0;
        for (Flux<MapTsItemVec> fl : fls) {
            int ii1 = i1;
            Flux<MapTsToken> fl2 = fl
            .takeWhile(MapTsItemVec::notTerm)
            .doOnNext(k -> k.markWith(BufCont.Mark.MERGER_SUPPORT_ITEM_VEC_01))
            .transform(QueryData.doDiscard("mergeItemVecFluxesA"))
            .concatMap(k -> k.intoFlux(ii1), qp.mergerSupportItemVecPrefetch)
            .doOnNext(k -> k.appendName(BufCont.Mark.MERGER_SUPPORT_ITEM_VEC_01))
            .transform(QueryData.doDiscard("mergeItemVecFluxesB"));
            ret.add(fl2);
            i1 += 1;
        }
        LOGGER.info("mergeItemVecFluxes  produced");
        return Flux.from(Merger.merge(new ArrayList<>(ret)));
    }

    static class Flatten {
        final AtomicLong totalBytes = new AtomicLong();
        final AtomicLong stop = new AtomicLong();
        String channelName;
        QueryParams qp;
        BufCtx bufCtx;
        boolean released;
        Flatten(String channelName, QueryParams qp, BufCtx bufCtx) {
            this.channelName = channelName;
            this.qp = qp;
            this.bufCtx = bufCtx;
        }
        List<BufCont> next(MapTsToken tok) {
            if (released) {
                LOGGER.error("Flatten  next  released {}", released);
                throw new RuntimeException("logic");
            }
            Output out = new Output(bufCtx.bufFac, bufCtx.bufferSize, BufCont.Mark.Flatten);
            if (qp.limitBytesPerChannel == 0 || tok.ty == MapTsItemVec.Ty.MIDDLE || tok.ty == MapTsItemVec.Ty.CLOSE || totalBytes.get() < qp.limitBytesPerChannel) {
                out.transfer(tok.bufcont.bufferRef().slice(tok.pos, tok.len), tok.len);
                totalBytes.getAndAdd(tok.len);
                tok.release();
            }
            else {
                LOGGER.info("Limit reached {}", totalBytes.get());
                stop.set(1);
            }
            return out.take();
        }
        void release() {
            released = true;
        }
    }

    public static Flux<BufCont> flattenSlices(Flux<MapTsToken> fls, String channelName, QueryParams qp, BufCtx bufCtx) {
        Flatten flatten = new Flatten(channelName, qp, bufCtx);
        return fls
        .takeWhile(tok -> tok.ts < qp.endNanos)
        .takeWhile(tok -> flatten.stop.get() == 0)
        .filter(tok -> tok.ts < qp.endNanos)
        .filter(k -> flatten.stop.get() == 0)
        .transform(QueryData.doDiscard("flattenSlices"))
        .concatMapIterable(flatten::next, qp.flattenSlicesPrefetch + 1)
        .doFinally(sig -> flatten.release())
        .doOnNext(k -> k.appendMark(BufCont.Mark.FlattenPassOn));
    }

}
