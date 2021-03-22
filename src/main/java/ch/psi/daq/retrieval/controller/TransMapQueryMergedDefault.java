package ch.psi.daq.retrieval.controller;

import ch.psi.daq.retrieval.QueryParams;
import ch.psi.daq.retrieval.reqctx.ReqCtx;
import ch.psi.daq.retrieval.bytes.BufCont;
import ch.psi.daq.retrieval.config.ChannelConfigEntry;
import ch.psi.daq.retrieval.eventmap.value.EventBlobMapResult;
import ch.psi.daq.retrieval.eventmap.value.EventBlobToV1Map;
import ch.psi.daq.retrieval.eventmap.value.TransformSupplier;
import ch.psi.daq.retrieval.pod.api1.Channel;
import reactor.core.publisher.Flux;

public class TransMapQueryMergedDefault implements TransformSupplier<BufCont> {

    public TransMapQueryMergedDefault(ReqCtx reqCtx, QueryParams qp) {
        this.reqCtx = reqCtx;
        this.qp = qp;
    }

    public Flux<BufCont> trans(Flux<BufCont> fl, Channel channel, ChannelConfigEntry configEntry) {
        return EventBlobToV1Map.trans(reqCtx, fl, channel.name, qp.endNanos, reqCtx.bufCtx, qp)
        .doOnNext(k -> k.appendMark(BufCont.Mark.TransMapQueryMerged_01))
        .concatMapIterable(EventBlobMapResult::takeBufCont, 1)
        .doOnNext(k -> k.appendMark(BufCont.Mark.TransMapQueryMerged_02));
    }

    QueryParams qp;
    ReqCtx reqCtx;

}
