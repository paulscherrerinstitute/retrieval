package ch.psi.daq.retrieval.finder;

import ch.psi.daq.retrieval.reqctx.ReqCtx;
import ch.psi.daq.retrieval.config.ChannelConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Implements traversing the original version 0 directory hierarchy as currently used
 * by databuffer, imagebuffer, ...
 */
public class BaseDirFinderFormatV0 {
    static final Logger LOGGER = LoggerFactory.getLogger(BaseDirFinderFormatV0.class.getSimpleName());
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

    public Mono<ScanDataFilesResult> findMatchingDataFiles(ReqCtx reqCtx, String channelName, Instant begin, Instant end, int split) {
        long beginMs = begin.toEpochMilli();
        long endMs = end.toEpochMilli();
        LocatedChannel locatedChannel = new LocatedChannel(new BaseDir(baseDir, baseKeyspaceName), channelName);
        return scanner.getChannelScan(reqCtx, locatedChannel, beginMs, endMs, split);
    }

    public static Mono<ChannelConfig> channelConfig(ReqCtx reqCtx, LocatedChannel locatedChannel) {
        long tsBeg = System.nanoTime();
        Path path = Path.of(String.format("%s/config/%s/latest/00000_Config", locatedChannel.base.baseDir, locatedChannel.name));
        if (!Files.exists(path)) {
            return Mono.error(new ConfigFileNotFound());
        }
        Flux<DataBuffer> fb = DataBufferUtils.readByteChannel(() -> {
            try {
                return Files.newByteChannel(path);
            }
            catch (IOException e) {
                LOGGER.info("{}  existed but can not open config {}  {}", reqCtx, locatedChannel.name, path);
                throw new RuntimeException(e);
            }
        }, reqCtx.bufCtx.bufFac, 1024 * 16);
        return DataBufferUtils.join(fb, 1<<21)
        .map(k -> {
            try {
                return Config.parseChannelConfig(reqCtx, locatedChannel, k);
            }
            finally {
                DataBufferUtils.release(k);
            }
        })
        .doOnNext(k -> {
            reqCtx.addConfigParseDuration(System.nanoTime() - tsBeg);
        });
    }

    public static class Stats {
        public long configNotFound;
        public long configReadError;
        public Stats() {
            configNotFound = BaseDirFinderFormatV0.configNotFound.get();
            configReadError = BaseDirFinderFormatV0.configReadError.get();
        }
    }

    public static final AtomicLong configNotFound = new AtomicLong();
    public static final AtomicLong configReadError = new AtomicLong();

}
