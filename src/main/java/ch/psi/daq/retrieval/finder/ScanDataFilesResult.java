package ch.psi.daq.retrieval.finder;

import ch.psi.daq.retrieval.config.ChannelConfigEntry;
import reactor.core.publisher.Flux;

import java.nio.file.Path;

public class ScanDataFilesResult {

    public ScanDataFilesResult(LocatedChannel locatedChannel, ChannelConfigEntry channelConfigEntry, int ks, int sp, Flux<TimeBin2> timeBins, long untilMs) {
        this.locatedChannel = locatedChannel;
        this.channelConfigEntry = channelConfigEntry;
        this.ks = ks;
        this.sp = sp;
        this.timeBins = timeBins;
        this.untilMs = untilMs;
    }

    public Path dataPath(TimeBin2 tb) {
        String p = String.format("%s/%s_%d/byTime/%s/%019d/%010d/%019d_%05d_Data", locatedChannel.base.baseDir, locatedChannel.base.baseKeyspaceName, ks, locatedChannel.name, tb.timeBin, sp, tb.binSize, 0);
        return Path.of(p);
    }

    public Path indexPath(TimeBin2 tb) {
        String p = String.format("%s/%s_%d/byTime/%s/%019d/%010d/%019d_%05d_Data", locatedChannel.base.baseDir, locatedChannel.base.baseKeyspaceName, ks, locatedChannel.name, tb.timeBin, sp, tb.binSize, 0);
        return Path.of(p);
    }

    public ChannelConfigEntry channelConfigEntry;
    public LocatedChannel locatedChannel;
    public int ks;
    public int sp;
    public Flux<TimeBin2> timeBins;
    public long untilMs;

}
