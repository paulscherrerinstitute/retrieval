package ch.psi.daq.retrieval.controller;

import ch.psi.daq.retrieval.KeyspaceToDataParams;
import ch.psi.daq.retrieval.MapFunctionFactory;
import ch.psi.daq.retrieval.QueryParams;
import ch.psi.daq.retrieval.ReqCtx;
import ch.psi.daq.retrieval.bytes.BufCont;
import ch.psi.daq.retrieval.eventmap.ts.EventBlobToV1MapTs;
import ch.psi.daq.retrieval.eventmap.ts.MapTsItemVec;
import reactor.core.publisher.Flux;

class TransMapTsForRaw implements MapFunctionFactory<MapTsItemVec> {
    ReqCtx reqctx;
    QueryParams qp;
    long endNanos;
    String channelName;

    public TransMapTsForRaw(ReqCtx reqctx, QueryParams qp, long endNanos, String channelName) {
        this.reqctx = reqctx;
        this.qp = qp;
        this.endNanos = endNanos;
        this.channelName = channelName;
    }

    @Override
    public Flux<MapTsItemVec> makeTrans(Flux<BufCont> fl, KeyspaceToDataParams kspp, int fileno) {
        return EventBlobToV1MapTs.trans(reqctx, fl, String.format("rawLocal_sp%02d/%d_f%02d", qp.splits.get(0), qp.splits
        .size(), fileno), channelName, endNanos, reqctx.bufCtx);
    }
}
