package ch.psi.daq.retrieval.finder;

import ch.psi.daq.retrieval.RangeTs;
import ch.psi.daq.retrieval.ReqCtx;
import ch.psi.daq.retrieval.config.ChannelConfigEntry;
import org.springframework.core.io.buffer.DataBufferFactory;
import reactor.core.publisher.Flux;

import java.util.List;

public class DatafileScan {

    public static Flux<String> scanForMapping(ReqCtx reqctx, StorageSettings sts, RangeTs rangeTs, List<String> channelNames, DataBufferFactory bufFac) {
        return Flux.fromIterable(channelNames)
        .map(k -> new Channel(new BaseDir(sts.baseDir, sts.ksPrefix), k))
        .concatMap(channel -> BaseDirFinderFormatV0.channelConfig(reqctx, channel, bufFac))
        .concatMapIterable(k -> {
            if (k.isPresent()) {
                return k.get().entries;
            }
            else {
                return List.of();
            }
        }, 1)
        .map(ChannelConfigEntry::toString);
    }

}
