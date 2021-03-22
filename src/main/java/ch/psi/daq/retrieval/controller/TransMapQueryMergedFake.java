package ch.psi.daq.retrieval.controller;

import ch.psi.daq.retrieval.QueryParams;
import ch.psi.daq.retrieval.reqctx.ReqCtx;
import ch.psi.daq.retrieval.bytes.BufCont;
import ch.psi.daq.retrieval.config.ChannelConfigEntry;
import ch.psi.daq.retrieval.eventmap.value.TransformSupplier;
import ch.psi.daq.retrieval.pod.api1.Channel;
import reactor.core.publisher.Flux;

public class TransMapQueryMergedFake implements TransformSupplier<BufCont> {

    public TransMapQueryMergedFake(ReqCtx reqctx, QueryParams qp) {
        this.reqctx = reqctx;
        this.qp = qp;
    }

    public Flux<BufCont> trans(Flux<BufCont> fl, Channel channel, ChannelConfigEntry configEntry) {
        return fl;
    }

    QueryParams qp;
    ReqCtx reqctx;

}
