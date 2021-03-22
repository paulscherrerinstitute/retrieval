package ch.psi.daq.retrieval.finder;

import ch.psi.daq.retrieval.reqctx.ReqCtx;
import ch.psi.daq.retrieval.config.ChannelConfig;
import ch.psi.daq.retrieval.config.ChannelConfigEntry;
import ch.psi.daq.retrieval.utils.DateExt;
import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;

public class BaseDirScanner {
    static final Logger LOGGER = (Logger) LoggerFactory.getLogger(BaseDirScanner.class.getSimpleName());
    static final long MS = 1000000L;

    public Mono<ScanDataFilesResult> getChannelScan(ReqCtx reqCtx, LocatedChannel locatedChannel, long beginMs, long endMs, int split) {
        return BaseDirFinderFormatV0.channelConfig(reqCtx, locatedChannel)
        .onErrorResume(e -> {
            if (e instanceof ConfigFileNotFound) {
                BaseDirFinderFormatV0.configNotFound.getAndIncrement();
            }
            else {
                BaseDirFinderFormatV0.configReadError.getAndIncrement();
                LOGGER.error("{}  error while reading config for {}  {}", reqCtx, locatedChannel, e.toString());
            }
            return Mono.empty();
        })
        .flatMap(k -> channelsFromConfig(reqCtx, locatedChannel, k, beginMs, endMs, split));
    }

    public Mono<ScanDataFilesResult> channelsFromConfig(ReqCtx reqCtx, LocatedChannel locatedChannel, ChannelConfig channelConfig, long beginMs, long endMs, int split) {
        return matchingConfigEntry(reqCtx, locatedChannel, channelConfig, beginMs, endMs, split)
        .map(ce -> {
            ChannelConfigEntry ee = ce.entry;
            long untilMs = ce.untilMs;
            long binCountMax = (endMs - beginMs) / ee.bs + 1;
            Flux<TimeBin2> fl = Flux.range(0, (int) binCountMax)
            .map(i1 -> {
                long bin = (beginMs + i1 * ee.bs) / ee.bs;
                TimeBin2 tb = new TimeBin2(bin, ee.bs);
                LOGGER.debug("{}  emit data timebin  ks {}  sp {}  tb {}  bs {}   {}", reqCtx, ee.ks, split, tb.timeBin, tb.binSize, locatedChannel.name);
                return tb;
            });
            return new ScanDataFilesResult(locatedChannel, ee, ee.ks, split, fl, untilMs);
        });
    }

    Mono<MatchingChannelConfigRange> matchingConfigEntry(ReqCtx reqCtx, LocatedChannel locatedChannel, ChannelConfig channelConfig, long beginMs, long endMs, int split) {
        //LOGGER.info("{}  matchingConfigEntry  channel {}", reqCtx, locatedChannel);
        if (channelConfig.entries.size() <= 0) {
            LOGGER.warn("{}  matchingConfigEntry  no entries  beginMs {}  endMs {}  split {}  channel {}", reqCtx, beginMs, endMs, split, locatedChannel);
            return Mono.empty();
        }
        else {
            List<ChannelConfigEntry> fittingEntries = new ArrayList<>();
            List<Long> untilMsList = new ArrayList<>();
            for (int i1 = 0; i1 < channelConfig.entries.size(); i1 += 1) {
                ChannelConfigEntry e1 = channelConfig.entries.get(i1);
                ChannelConfigEntry e2 = null;
                long e1Ms = e1.ts / MS;
                long e2Ms = 0;
                if (i1 + 1 < channelConfig.entries.size()) {
                    e2 = channelConfig.entries.get(i1 + 1);
                    e2Ms = e2.ts / MS;
                }
                if (e1Ms <= beginMs) {
                    if (e2 != null) {
                        if (e2Ms >= beginMs) {
                            fittingEntries.add(e1);
                            untilMsList.add(e2Ms);
                        }
                    }
                    else {
                        fittingEntries.add(e1);
                        untilMsList.add(Long.MAX_VALUE / MS);
                    }
                }
                else if (e1Ms <= endMs) {
                    if (e2 != null) {
                        fittingEntries.add(e1);
                        untilMsList.add(e2Ms);
                    }
                    else {
                        fittingEntries.add(e1);
                        untilMsList.add(Long.MAX_VALUE / MS);
                    }
                }
            }
            if (false) {
                LOGGER.info("{}  {}", reqCtx, formatEntries(locatedChannel, beginMs, endMs, split, fittingEntries));
            }
            if (fittingEntries.size() < 1) {
                //LOGGER.info("{}  no matching config found for {}", reqCtx, locatedChannel);
                return Mono.empty();
            }
            else {
                // TODO check compatibility:  same keyspace, same data type and shape.
                // TODO limit the request range to the config-compatible range
                if (fittingEntries.size() > 1) {
                    LOGGER.warn("{}  multiple config ranges  count {}  beginMs {}  endMs {}  channel {}", reqCtx, fittingEntries.size(), DateExt.toString(beginMs * 1000 * 1000), DateExt.toString(endMs * 1000 * 1000), locatedChannel.name);
                    LOGGER.warn("{}  multiple config ranges: {}", reqCtx, formatEntries(locatedChannel, beginMs, endMs, split, fittingEntries));
                }
                return Mono.just(new MatchingChannelConfigRange(fittingEntries.get(0), untilMsList.get(0)));
            }
        }
    }

    static StringBuilder formatEntries(LocatedChannel locatedChannel, long beginMs, long endMs, int split, List<ChannelConfigEntry> entries) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("found entries %d  channel %s  split %d\n", entries.size(), locatedChannel.name, split));
        sb.append(String.format("beginMs %s   endMs %s\n", DateExt.toString(beginMs * 1000 * 1000), DateExt.toString(endMs * 1000 * 1000)));
        for (ChannelConfigEntry e : entries) {
            sb.append(String.format("  ts %s    pulse %d\n", DateExt.toString(e.ts), e.pulse));
        }
        return sb;
    }

}
