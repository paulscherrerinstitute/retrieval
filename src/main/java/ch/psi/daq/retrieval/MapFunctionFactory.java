package ch.psi.daq.retrieval;

import org.springframework.core.io.buffer.DataBuffer;
import reactor.core.publisher.Flux;

public interface MapFunctionFactory<T> {
    Flux<T> makeTrans(Flux<DataBuffer> fl, KeyspaceToDataParams kspp, int fileno);
}
