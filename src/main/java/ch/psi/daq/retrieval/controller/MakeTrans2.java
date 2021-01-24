package ch.psi.daq.retrieval.controller;

import ch.psi.daq.retrieval.KeyspaceToDataParams;
import ch.psi.daq.retrieval.MapFunctionFactory;
import ch.psi.daq.retrieval.bytes.BufCont;
import reactor.core.publisher.Flux;

class MakeTrans2 implements MapFunctionFactory<BufCont> {
    @Override
    public Flux<BufCont> makeTrans(Flux<BufCont> fl, KeyspaceToDataParams kspp, int fileno) {
        return fl;
    }
}
