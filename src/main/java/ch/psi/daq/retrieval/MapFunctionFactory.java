package ch.psi.daq.retrieval;

import ch.psi.daq.retrieval.bytes.BufCont;
import reactor.core.publisher.Flux;

public interface MapFunctionFactory<T> {
    Flux<T> makeTrans(Flux<BufCont> fl, KeyspaceToDataParams kspp, int fileno);
}
