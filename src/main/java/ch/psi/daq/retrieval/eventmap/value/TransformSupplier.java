package ch.psi.daq.retrieval.eventmap.value;

import org.springframework.core.io.buffer.DataBuffer;
import reactor.core.publisher.Flux;

public interface TransformSupplier<T> {

    Flux<T> trans3(Flux<DataBuffer> fl, String channelName);

}
