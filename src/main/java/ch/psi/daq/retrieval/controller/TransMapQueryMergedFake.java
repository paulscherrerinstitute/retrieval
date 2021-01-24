package ch.psi.daq.retrieval.controller;

import ch.psi.daq.retrieval.QueryParams;
import ch.psi.daq.retrieval.ReqCtx;
import ch.psi.daq.retrieval.bytes.BufCont;
import ch.psi.daq.retrieval.eventmap.value.TransformSupplier;
import reactor.core.publisher.Flux;

class TransMapQueryMergedFake implements TransformSupplier<BufCont> {
    QueryParams qp;
    ReqCtx reqctx;

    TransMapQueryMergedFake(ReqCtx reqctx, QueryParams qp) {
        this.reqctx = reqctx;
        this.qp = qp;
    }

    public Flux<BufCont> trans3(Flux<BufCont> fl, String channelName) {
        return fl;
    }
}
