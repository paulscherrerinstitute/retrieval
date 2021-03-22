package ch.psi.daq.retrieval.controller;

import ch.psi.daq.retrieval.KeyspaceToDataParams;
import ch.psi.daq.retrieval.merger.Releasable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

@FunctionalInterface
interface ChannelTransform<T extends Releasable> {
    Mono<List<Flux<T>>> apply(KeyspaceToDataParams ksp);
}
