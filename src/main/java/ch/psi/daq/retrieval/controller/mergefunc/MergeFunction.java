package ch.psi.daq.retrieval.controller.mergefunc;

import ch.psi.daq.retrieval.pod.api1.Channel;
import ch.psi.daq.retrieval.reqctx.BufCtx;
import ch.psi.daq.retrieval.QueryParams;
import ch.psi.daq.retrieval.reqctx.ReqCtx;
import ch.psi.daq.retrieval.config.ChannelConfigEntry;
import ch.psi.daq.retrieval.eventmap.ts.MapTsItemVec;
import ch.psi.daq.retrieval.eventmap.ts.MapTsToken;
import reactor.core.publisher.Flux;

import java.util.List;

public interface MergeFunction {

    Flux<MapTsToken> apply(ReqCtx reqctx, List<Flux<MapTsToken>> lfl, ChannelConfigEntry configEntry, Channel channel, QueryParams qp, BufCtx bufCtx);

}
