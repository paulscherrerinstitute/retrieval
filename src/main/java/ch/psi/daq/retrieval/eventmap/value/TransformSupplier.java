package ch.psi.daq.retrieval.eventmap.value;

import ch.psi.daq.retrieval.bytes.BufCont;
import reactor.core.publisher.Flux;

public interface TransformSupplier<T> {

    Flux<T> trans3(Flux<BufCont> fl, String channelName);

}
