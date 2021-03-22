package ch.psi.daq.retrieval.eventmap.value;

import ch.psi.daq.retrieval.bytes.BufCont;
import ch.psi.daq.retrieval.config.ChannelConfigEntry;
import ch.psi.daq.retrieval.pod.api1.Channel;
import reactor.core.publisher.Flux;

public interface TransformSupplier<T> {

    Flux<T> trans(Flux<BufCont> fl, Channel channel, ChannelConfigEntry configEntry);

}
