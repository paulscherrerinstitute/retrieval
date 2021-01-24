package ch.psi.daq.retrieval.controller;

import org.springframework.core.io.buffer.DataBuffer;
import reactor.core.publisher.Flux;

class SubStream {
    String reqId;
    Flux<DataBuffer> fl;
}
