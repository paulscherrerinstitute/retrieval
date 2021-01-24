package ch.psi.daq.retrieval.controller;

import ch.psi.daq.retrieval.QueryParams;
import ch.psi.daq.retrieval.bytes.BufCont;
import ch.psi.daq.retrieval.eventmap.value.EventBlobToJsonMap;
import ch.psi.daq.retrieval.eventmap.value.MapJsonResult;
import ch.psi.daq.retrieval.eventmap.value.TransformSupplier;
import reactor.core.publisher.Flux;

class TransformSup3 implements TransformSupplier<MapJsonResult> {
    QueryParams qp;

    TransformSup3(QueryParams qp) {
        this.qp = qp;
    }

    public Flux<MapJsonResult> trans3(Flux<BufCont> fl, String channelName) {
        return EventBlobToJsonMap.trans2(fl, channelName, qp.endNanos, qp.bufFac, qp.bufferSize, qp.limitBytes);
    }

}
