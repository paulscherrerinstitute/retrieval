package ch.psi.daq.retrieval.subnodes;

import ch.psi.daq.retrieval.config.ChannelConfigEntry;
import ch.psi.daq.retrieval.eventmap.ts.MapTsToken;
import reactor.core.publisher.Flux;

import java.util.List;

class ConfigSubFlux {

    ConfigSubFlux(ChannelConfigEntry configEntry, List<Flux<MapTsToken>> fll) {
        this.configEntry = configEntry;
        this.fll = fll;
    }

    ChannelConfigEntry configEntry;
    List<Flux<MapTsToken>> fll;

}
