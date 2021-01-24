package ch.psi.daq.retrieval.controller;

import ch.psi.daq.retrieval.QueryParams;
import ch.psi.daq.retrieval.ReqCtx;
import ch.psi.daq.retrieval.bytes.BufCont;
import ch.psi.daq.retrieval.eventmap.value.EventBlobMapResult;
import ch.psi.daq.retrieval.eventmap.value.EventBlobToV1Map;
import ch.psi.daq.retrieval.eventmap.value.TransformSupplier;
import reactor.core.publisher.Flux;

class TransMapQueryMergedDefault implements TransformSupplier<BufCont> {
    QueryParams qp;
    ReqCtx reqctx;

    TransMapQueryMergedDefault(ReqCtx reqctx, QueryParams qp) {
        this.reqctx = reqctx;
        this.qp = qp;
    }

    public Flux<BufCont> trans3(Flux<BufCont> fl, String channelName) {
        return EventBlobToV1Map.trans(reqctx, fl, channelName, qp.endNanos, qp.bufFac, qp.bufferSize, qp)
        .doOnNext(k -> k.appendName(BufCont.Mark.TransMapQueryMerged_01))
        .concatMapIterable(EventBlobMapResult::takeBufCont, 1)
        .doOnNext(k -> k.appendMark(BufCont.Mark.TransMapQueryMerged_02));
    }
}
