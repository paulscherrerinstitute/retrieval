package ch.psi.daq.retrieval.finder;

import ch.psi.daq.retrieval.ReqCtx;
import ch.psi.daq.retrieval.config.ChannelConfig;
import ch.psi.daq.retrieval.config.ChannelConfigEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DataBufferUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Implements traversing the original version 0 directory hierarchy as currently used
 * by databuffer, imagebuffer, ...
 */
public class BaseDirFinderFormatV0 {
    static final Logger LOGGER = LoggerFactory.getLogger(BaseDirFinderFormatV0.class);
    static final BaseDirScanner scanner = new BaseDirScanner();
    final Path baseDir;
    final String baseKeyspaceName;

    public BaseDirFinderFormatV0(Path baseDir, String baseKeyspaceName) {
        this.baseDir = baseDir;
        this.baseKeyspaceName = baseKeyspaceName;
    }

    public static class TimeBin {
        TimeBin(long timeBin) {
            this.timeBin = timeBin;
        }
        public long timeBin;
        public List<SplitBinsize> splits;
    }

    public static class SplitBinsize {
        public long split;
        public long binsize;
        public boolean hasIndex = false;
        static SplitBinsize create(long split, long binsize, boolean hasIndex) {
            SplitBinsize ret = new SplitBinsize();
            ret.split = split;
            ret.binsize = binsize;
            ret.hasIndex = hasIndex;
            return ret;
        }
    }

    public static class MatchingDataFilesResult {
        public String channelName;
        public Instant begin;
        public Instant end;
        public List<KeyspaceOrder2> keyspaces;
    }

    public Mono<MatchingDataFilesResult> findMatchingDataFiles(ReqCtx reqctx, String channelName, Instant begin, Instant end, List<Integer> splits, DataBufferFactory bufFac) {
        long beginMs = begin.toEpochMilli();
        long endMs = end.toEpochMilli();
        Channel channel = new Channel(new BaseDir(baseDir, baseKeyspaceName), channelName);
        return scanDataFiles(reqctx, channel, beginMs, endMs, splits, bufFac)
        .map(res -> {
            List<KeyspaceOrder2> lKsp = res.keyspaces.stream()
            .map(ks -> {
                List<Split> lSp = ks.splits.stream()
                .map(sp -> {
                    List<TimeBin2> lTb = sp.timeBins.stream()
                    //.peek(tb -> {
                    //    LOGGER.debug("{}  candidate  channel {}  split {}  bin {}  binSize {}", reqctx, channel, sp.split, tb.timeBin, tb.binSize);
                    //})
                    .filter(tb -> {
                        long beg = tb.timeBin * tb.binSize;
                        boolean pr = beg < endMs && (beg + tb.binSize > beginMs);
                        return pr;
                    })
                    .peek(tb -> {
                        LOGGER.debug("{}  chosen  channel {}  split {}  bin {}  binSize {}", reqctx, channel, sp.split, tb.timeBin, tb.binSize);
                    })
                    .collect(Collectors.toList());
                    Split sp2 = new Split(sp.split);
                    sp2.timeBins = lTb;
                    return sp2;
                })
                .filter(x -> x.timeBins.size() > 0)
                .collect(Collectors.toList());
                KeyspaceOrder2 ret = new KeyspaceOrder2(ks.ksp);
                ret.splits = lSp;
                ret.channel = ks.channel;
                return ret;
            })
            .filter(x -> x.splits.size() > 0)
            .collect(Collectors.toList());
            if (lKsp.size() > 1) {
                throw new RuntimeException("Channel must be only in one keyspace");
            }
            MatchingDataFilesResult ret = new MatchingDataFilesResult();
            ret.channelName = res.channelName;
            ret.begin = begin;
            ret.end = end;
            ret.keyspaces = lKsp;
            return ret;
        });
    }

    public Mono<ScanDataFilesResult> scanDataFiles(ReqCtx reqctx, Channel channel, long beginMs, long endMs, List<Integer> splits, DataBufferFactory bufFac) {
        return scanner.getChannelScan(reqctx, channel, beginMs, endMs, splits, bufFac);
    }

    public static Mono<Optional<ChannelConfig>> channelConfig(ReqCtx reqctx, Channel channel, DataBufferFactory bufFac) {
        long tsBeg = System.nanoTime();
        Path path = Path.of(String.format("%s/config/%s/latest/00000_Config", channel.base.baseDir, channel.name));
        Flux<DataBuffer> fb = DataBufferUtils.readByteChannel(() -> {
            try {
                return Files.newByteChannel(path);
            }
            catch (IOException e) {
                LOGGER.warn("{}  can not open {}", reqctx, path);
                throw new RuntimeException(e);
            }
        }, bufFac, 512 * 1024);
        return DataBufferUtils.join(fb, 1<<21)
        .map(Optional::of)
        .onErrorReturn(Optional.empty())
        .map(x -> {
            if (x.isEmpty()) {
                LOGGER.warn("{}  could not read config for {}", reqctx, path);
                return Optional.empty();
            }
            DataBuffer buf = x.get();
            ByteBuffer bbuf = buf.asByteBuffer(buf.readPosition(), buf.readableByteCount());
            short vser = bbuf.getShort();
            if (vser != 0) {
                LOGGER.error("{}  bad config ser {} {}", reqctx, channel.name, vser);
                throw new RuntimeException("logic error");
            }
            int channelNameLength1 = bbuf.getInt();
            if (channelNameLength1 < 8) {
                LOGGER.error("{}  channel name bad  {}  {}", reqctx, channel.name, channelNameLength1);
                throw new RuntimeException("logic");
            }
            channelNameLength1 -= 8;
            StandardCharsets.UTF_8.decode(bbuf.slice().limit(channelNameLength1));
            bbuf.position(bbuf.position() + channelNameLength1);
            if (bbuf.getInt() != channelNameLength1 + 8) {
                LOGGER.error("{}  channel name bad  {}", reqctx, channelNameLength1);
                throw new RuntimeException("logic");
            }
            ChannelConfig conf = new ChannelConfig();
            while (bbuf.remaining() > 0) {
                int p1 = bbuf.position();
                int len1 = bbuf.getInt();
                if (len1 < 32 || len1 > 1024) {
                    LOGGER.error("{}  bad config entry len  {}", reqctx, len1);
                    throw new RuntimeException("logic");
                }
                if (bbuf.remaining() < len1 - Integer.BYTES) {
                    LOGGER.error("bad config entry len  {}", len1);
                    throw new RuntimeException("logic");
                }
                long ts = bbuf.getLong();
                long pulse = bbuf.getLong();
                int ks = bbuf.getInt();
                long bs = bbuf.getLong();
                int sc = bbuf.getInt();
                LOGGER.debug("{} found meta  ts {}  ks {}  bs {}  sc {}", reqctx, ts, ks, bs, sc);
                ChannelConfigEntry e = new ChannelConfigEntry();
                e.ts = ts;
                e.ks = ks;
                e.bs = bs;
                e.sc = sc;
                conf.entries.add(e);
                bbuf.position(p1 + len1);
            }
            DataBufferUtils.release(buf);
            reqctx.addConfigParseDuration(System.nanoTime() - tsBeg);
            return Optional.of(conf);
        });
    }

}
