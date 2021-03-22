package ch.psi.daq.retrieval.subnodes;

import ch.psi.daq.retrieval.bytes.BufCont;
import reactor.core.publisher.Flux;

class SubStream {

    String reqId;
    Flux<BufCont> fl;

}
