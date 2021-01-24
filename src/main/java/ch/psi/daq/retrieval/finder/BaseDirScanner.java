package ch.psi.daq.retrieval.finder;

import ch.psi.daq.retrieval.CacheLRU;
import ch.psi.daq.retrieval.ReqCtx;
import ch.psi.daq.retrieval.config.ChannelConfig;
import ch.psi.daq.retrieval.Microseconds;
import ch.psi.daq.retrieval.config.ChannelConfigEntry;
import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBufferFactory;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BaseDirScanner {
    static final Logger LOGGER = (Logger) LoggerFactory.getLogger(BaseDirScanner.class.getSimpleName());
    final CacheLRU<String, ScanDataFilesResult> cacheForChannel = new CacheLRU<>(1<<15);
    static final List<String> keyspaces = List.of("2", "3", "4");
    static final Pattern patternTimeBin = Pattern.compile("[0-9]{19}");
    static final Pattern patternSplit = Pattern.compile("[0-9]{10}");
    static final Pattern patternDatafile = Pattern.compile("([0-9]{19})_([0-9]{5})_Data");

    public Mono<ScanDataFilesResult> getChannelScan(ReqCtx reqctx, Channel channel, long beginMs, long endMs, List<Integer> splits, DataBufferFactory bufFac) {
        LOGGER.debug("{}  beginMs: {}  endMs: {}", reqctx, beginMs, endMs);
        return BaseDirFinderFormatV0.channelConfig(reqctx, channel, bufFac)
        .flatMap(x -> {
            if (x.isPresent()) {
                LOGGER.info("{}  got config  {}", reqctx, channel);
                ChannelConfig conf = x.get();
                ScanDataFilesResult res = new ScanDataFilesResult();
                res.channelName = channel.name;
                res.keyspaces = new ArrayList<>();
                if (conf.entries.size() <= 0) {
                    LOGGER.warn("{}  conf has no entries", reqctx);
                }
                else {
                    // Find the first config entry before or at beginMs
                    // Use that keyspace from there...
                    for (int i1 = 0; i1 < conf.entries.size(); i1 += 1) {
                        ChannelConfigEntry e1 = conf.entries.get(i1);
                        LOGGER.trace("{}  ChannelConfigEntry e1  ix {}  ts {}  bs {}  bin {}", reqctx, i1, e1.ts / 1000000, e1.bs, e1.ts / 1000000 / e1.bs);
                        ChannelConfigEntry e2 = null;
                        long toMs = endMs;
                        if (i1 < conf.entries.size() - 1) {
                            e2 = conf.entries.get(i1 + 1);
                            toMs = Math.min(toMs, e2.ts / 1000000);
                            LOGGER.trace("{}  ChannelConfigEntry e2  ix {}  ts {}  bs {}  bin {}", reqctx, i1 + 1, e2.ts / 1000000, e2.bs, e2.ts / 1000000 / e2.bs);
                            if (e2.ts / 1000000 <= beginMs) {
                                LOGGER.trace("{}  disregard ix {}", reqctx, i1 + 1);
                                continue;
                            }
                        }
                        LOGGER.debug("{}  toMs: {}", reqctx, toMs);
                        if (res.keyspaces.size() == 0) {
                            KeyspaceOrder2 ksp = new KeyspaceOrder2(String.format("%d", e1.ks));
                            ksp.channel = channel;
                            if (splits.isEmpty()) {
                                ksp.splits = new ArrayList<>();
                                for (int spi = 0; spi < e1.sc; spi += 1) {
                                    Split s = new Split(spi);
                                    s.timeBins = new ArrayList<>();
                                    ksp.splits.add(s);
                                }
                            }
                            else {
                                ksp.splits = splits.stream().map(sp -> {
                                    Split s = new Split(sp);
                                    s.timeBins = new ArrayList<>();
                                    return s;
                                }).collect(Collectors.toList());
                            }
                            res.keyspaces.add(ksp);
                        }
                        LOGGER.trace("\nbeginMs: {}\ne1.ts:   {}\ntoMs:    {}", beginMs, e1.ts / 1000000, toMs);
                        long tsMs = (Math.max(beginMs, e1.ts / 1000000) / e1.bs) * e1.bs;
                        for (; tsMs < toMs; tsMs += e1.bs) {
                            long bin = tsMs / e1.bs;
                            LOGGER.debug("{}  add file  tsMs: {}  bin: {}  next tsMs: {}", reqctx, tsMs, bin, tsMs + e1.bs);
                            for (KeyspaceOrder2 ksp2 : res.keyspaces) {
                                LOGGER.trace("{}  Look at ksp2  ks {}  splits {}", reqctx, ksp2.ksp, ksp2.splits);
                                for (Split split : ksp2.splits) {
                                    int dummy = 0;
                                    String fn = String.format("%s/%s_%s/byTime/%s/%019d/%010d/%019d_%05d_Data_Index", channel.base.baseDir, channel.base.baseKeyspaceName, ksp2.ksp, channel.name, bin, split.split, e1.bs, dummy);
                                    boolean hasIndex = Files.isReadable(Path.of(fn));
                                    TimeBin2 tb = new TimeBin2(bin, e1.bs, hasIndex);
                                    boolean alreadyThere = false;
                                    for (TimeBin2 tb2 : split.timeBins) {
                                        if (tb2.timeBin == tb.timeBin && tb2.binSize == tb.binSize) {
                                            alreadyThere = true;
                                            break;
                                        }
                                    }
                                    if (!alreadyThere) {
                                        split.timeBins.add(tb);
                                    }
                                }
                            }
                        }
                    }
                }
                LOGGER.debug("{}  summary  ks size {}", reqctx, res.keyspaces.size());
                if (res.keyspaces.size() > 0) {
                    LOGGER.debug("{}  summary  ks size {}  ksp {}", reqctx, res.keyspaces.size(), res.keyspaces.get(0).ksp);
                    if (res.keyspaces.get(0).splits.size() > 0) {
                        LOGGER.debug("{}  summary  ks size {}  ksp {}  tb size {}", reqctx, res.keyspaces.size(), res.keyspaces
                        .get(0).ksp, res.keyspaces.get(0).splits.get(0).timeBins.size());
                    }
                }
                return Mono.just(res);
            }
            else {
                LOGGER.warn("{}  channel configuration not found  {}", reqctx, channel);
                if (false) {
                    return scanDataFiles3(reqctx, channel);
                }
                ScanDataFilesResult res = new ScanDataFilesResult();
                res.channelName = channel.name;
                res.keyspaces = List.of();
                return Mono.just(res);
            }
        });
    }

    public Mono<ScanDataFilesResult> scanDataFiles3(ReqCtx reqctx, Channel channel) {
        Mono<ScanDataFilesResult> mfind = Mono.fromCallable(() -> {
            LOGGER.debug("{}  scanDataFiles3 {}", reqctx, channel);
            long tsScanBegin = System.nanoTime();
            List<KeyspaceOrder1> foundKeyspaces = keyspaces.stream()
            .map(x -> Tuples.of(x, Path.of(String.format("%s/%s/%s_%s/byTime/%s", channel.base.baseDir, channel.base.baseKeyspaceName, channel.base.baseKeyspaceName, x, channel.name))))
            .map(x -> {
                try {
                    String ksstr = x.getT1();
                    Path pathChannel = x.getT2();
                    List<Path> timeBinPaths;
                    try (Stream<Path> stream = Files.list(pathChannel)) {
                        timeBinPaths = stream.collect(Collectors.toList());
                    }
                    List<BaseDirFinderFormatV0.TimeBin> timeBins = timeBinPaths.stream().sequential()
                    .map(x2 -> {
                        String dirString = x2.getFileName().toString();
                        if (patternTimeBin.matcher(dirString).matches()) {
                            long bin = Long.parseLong(dirString);
                            return Optional.of(bin);
                        }
                        else {
                            return Optional.<Long>empty();
                        }
                    })
                    .filter(Optional::isPresent).map(Optional::get)
                    .map(timeBin -> {
                        try {
                            List<Path> splitPaths;
                            try (Stream<Path> stream = Files.list(pathChannel.resolve(String.format("%019d", timeBin)))) {
                                splitPaths = stream.collect(Collectors.toList());
                            }
                            List<BaseDirFinderFormatV0.SplitBinsize> listOfSplits = splitPaths.stream().sequential()
                            .flatMap(de -> {
                                String s = de.getFileName().toString();
                                if (patternSplit.matcher(s).matches()) {
                                    Long split = Long.parseLong(s);
                                    List<Path> datafilePaths;
                                    try {
                                        try (Stream<Path> stream = Files.list(pathChannel.resolve(String.format("%019d", timeBin)).resolve(String.format("%010d", split)))) {
                                            datafilePaths = stream.collect(Collectors.toList());
                                        }
                                        return datafilePaths.stream().sequential()
                                        .map(datafilename -> patternDatafile.matcher(datafilename.getFileName().toString()))
                                        .filter(Matcher::matches)
                                        .map(m -> {
                                            long bs = Long.parseLong(m.group(1));
                                            boolean ex = Files.exists(pathChannel.resolve(String.format("%019d", timeBin)).resolve(String.format("%010d", split)).resolve(String.format("%019d_%05d_Data_Index", bs, 0)));
                                            return BaseDirFinderFormatV0.SplitBinsize.create(split, bs, ex);
                                        });
                                    }
                                    catch (IOException e) {
                                        throw new RuntimeException(e);
                                    }
                                }
                                else {
                                    return Stream.empty();
                                }
                            })
                            .collect(Collectors.toList());
                            BaseDirFinderFormatV0.TimeBin s1 = new BaseDirFinderFormatV0.TimeBin(timeBin);
                            s1.splits = listOfSplits;
                            return s1;
                        }
                        catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .collect(Collectors.toList());
                    KeyspaceOrder1 ks = new KeyspaceOrder1(ksstr);
                    ks.timeBins = timeBins;
                    return Optional.of(ks);
                }
                catch (IOException e) {
                    return Optional.<KeyspaceOrder1>empty();
                }
            })
            .filter(Optional::isPresent).map(Optional::get)
            .collect(Collectors.toList());
            List<KeyspaceOrder2> keyspaces2 = foundKeyspaces.stream()
            .map(ks1 -> {
                Map<Long, Split> m23 = ks1.timeBins.stream().flatMap(x -> x.splits.stream().map(sp -> sp.split)).collect(Collectors.toSet()).stream()
                .map(s -> Tuples.of(s, new Split(s.intValue())))
                .collect(Collectors.toMap(Tuple2::getT1, Tuple2::getT2));
                ks1.timeBins.forEach(tb -> tb.splits.forEach(sp -> m23.get(sp.split).timeBins.add(new TimeBin2(tb.timeBin, sp.binsize, sp.hasIndex))));
                KeyspaceOrder2 ks2 = new KeyspaceOrder2(ks1.ksp);
                ks2.splits = new ArrayList<>(m23.values());
                ks2.channel = channel;
                return ks2;
            })
            .collect(Collectors.toList());
            ScanDataFilesResult ret = new ScanDataFilesResult();
            ret.channelName = channel.name;
            ret.keyspaces = keyspaces2;
            ret.duration = Microseconds.fromNanos(System.nanoTime() - tsScanBegin);
            return ret;
        });
        return mfind;
    }

    public BaseDirScanner() {
    }

}
