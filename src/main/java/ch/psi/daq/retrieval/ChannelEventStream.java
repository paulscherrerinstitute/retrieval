package ch.psi.daq.retrieval;

import ch.psi.daq.retrieval.finder.Channel;
import ch.psi.daq.retrieval.finder.KeyspaceOrder2;
import ch.psi.daq.retrieval.finder.TimeBin2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DataBufferUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuples;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

public class ChannelEventStream {
    static final Logger LOGGER = LoggerFactory.getLogger(ChannelEventStream.class);
    DataBufferFactory bufFac;

    public ChannelEventStream(DataBufferFactory bufFac) {
        this.bufFac = bufFac;
    }

    public static <T> Mono<List<Flux<T>>> dataFluxFromFiles(KeyspaceToDataParams toDataParams, MapFunctionFactory<T> transFac) {
        ReqCtx reqctx = toDataParams.reqctx;
        long beginNanos = toDataParams.begin.toEpochMilli() * 1000000L;
        return Flux.fromIterable(toDataParams.ksp.splits)
        .filter(x -> toDataParams.splits.isEmpty() || toDataParams.splits.contains(x.split))
        .map(sp -> {
            LOGGER.debug("{}  open split {}", reqctx, sp.split);
            sp.timeBins.sort(TimeBin2::compareTo);
            KeyspaceOrder2 ksp = toDataParams.ksp;
            return Flux.fromIterable(sp.timeBins)
            .map(tb -> {
                return Tuples.of(
                    tb,
                    String.format("%s/%s_%s/byTime/%s/%019d/%010d/%019d_%05d_Data", ksp.channel.base.baseDir, ksp.channel.base.baseKeyspaceName, ksp.ksp, ksp.channel.name, tb.timeBin, sp.split, tb.binSize, 0),
                    sp.split
                );
            })
            .map(x -> Tuples.of(x.getT1(), Path.of(x.getT2()), x.getT3()))
            .index()
            .map(x -> {
                int fileno = (int) (long) x.getT1();
                int split = x.getT2().getT3();
                Path path = x.getT2().getT2();
                try {
                    long fsize = Files.size(path);
                    LOGGER.debug("{}  open datafile  {}  {}  {}", reqctx, x.getT1(), fsize, path);
                    if (x.getT2().getT1().hasIndex) {
                        reqctx.channelFileFound(new Channel(ksp.channel.base, ksp.channel.name), path);
                        return Optional.of(PositionedDatafile.openAndPosition(reqctx, path, beginNanos, fileno, split));
                    }
                    else {
                        reqctx.channelFileFound(new Channel(ksp.channel.base, ksp.channel.name), path);
                        return Optional.of(PositionedDatafile.openAndPositionNoIndex(reqctx, path, beginNanos, fileno, split));
                    }
                }
                catch (IOException e) {
                    LOGGER.debug("{}  datafile not present  {}  {}", reqctx, x.getT1(), path);
                    reqctx.channelFileNotFound(new Channel(ksp.channel.base, ksp.channel.name), path);
                    return Optional.<Mono<PositionedDatafile>>empty();
                }
            })
            .doOnNext(x -> {
                LOGGER.debug("{}  dataFluxFromFiles  present {}  channel {}  split {}", reqctx, x.isPresent(), ksp.channel.name, sp.split);
            })
            .filter(Optional::isPresent)
            .concatMap(opt -> {
                LOGGER.debug("{}  dataFluxFromFiles prepare openAndPosition Mono before concatMap", reqctx);
                if (opt == null) {
                    throw new RuntimeException("null ptr");
                }
                return opt.get();
            }, 1)
            .concatMap(f -> {
                LOGGER.debug("{}  read byte channel with buffersize {}  fileno {}", reqctx, toDataParams.bufferSize, f.fileno);
                Flux<DataBuffer> fl = DataBufferUtils.readByteChannel(() -> f.takeChannel(), toDataParams.bufFac, toDataParams.bufferSize)
                .doOnDiscard(Object.class, obj -> {
                    LOGGER.error("{}  DISCARD 982u3roiajcsjfo", reqctx);
                });
                return transFac.makeTrans(fl, toDataParams, f.fileno);
            }, 1);
        })
        .collectList();
    }

}
