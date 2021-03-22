package ch.psi.daq.retrieval.utils;

import ch.psi.daq.retrieval.bytes.BufCont;
import ch.psi.daq.retrieval.config.ChannelConfig;
import ch.psi.daq.retrieval.config.ChannelConfigEntry;
import reactor.core.publisher.Flux;

public class SplitDataFlux {

    public SplitDataFlux(ChannelConfigEntry channelConfigEntry, int split, Flux<BufCont> fl) {
        this.channelConfigEntry = channelConfigEntry;
        this.split = split;
        this.fl = fl;
    }

    public ChannelConfigEntry channelConfigEntry;
    public int split;
    public Flux<BufCont> fl;

}
