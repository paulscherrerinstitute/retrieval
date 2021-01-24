package ch.psi.daq.retrieval;

import ch.psi.daq.retrieval.finder.BaseDir;
import ch.psi.daq.retrieval.finder.KeyspaceOrder2;
import com.google.common.io.BaseEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuples;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class PositionedDatafile {
    static final Logger LOGGER = LoggerFactory.getLogger(PositionedDatafile.class.getSimpleName());

    public SeekableByteChannel channel;
    public Path path;
    public int fileno;

    PositionedDatafile(SeekableByteChannel channel, Path path, int fileno) {
        this.path = path;
        this.channel = channel;
        this.fileno = fileno;
    }

    /**
     * @param channel The byte channel.
     * @param path For error handling, remember the path of the byte channel.
     */
    public static PositionedDatafile fromChannel(SeekableByteChannel channel, Path path, int fileno) {
        return new PositionedDatafile(channel, path, fileno);
    }

    public static Mono<PositionedDatafile> openAndPosition(ReqCtx reqctx, Path path, long beginNano) {
        return openAndPosition(reqctx, path, beginNano, -1, 0xcafe);
    }

    public static Mono<PositionedDatafile> openAndPosition(ReqCtx reqctx, Path path, long beginNano, int fileno, int split) {
        return Index.openIndex(Path.of(path.toString() + "_Index"))
        .map(x -> Index.findGEByLong(beginNano, x))
        .flatMap(x -> {
            LOGGER.trace("findGEByLong  {}  {}  {}", x.i, x.k, x.v);
            return Mono.fromCallable(() -> {
                SeekableByteChannel c = Files.newByteChannel(path, StandardOpenOption.READ);
                long v;
                long e = c.size();
                if (x.isSome()) {
                    v = x.v;
                }
                else {
                    v = e;
                }
                LOGGER.debug("{}  Seek  fileno {}  split {}  position {} / {}   path {}", reqctx, fileno, split, v, e, path);
                c.position(v);
                LOGGER.debug("{}  positioned    ix  beginNano {}  fileno {}  split {}  path {}  pos {}", reqctx, beginNano, fileno, split, path, v);
                return PositionedDatafile.fromChannel(c, path, fileno);
            })
            .timeout(Duration.ofMillis(4000))
            .doOnError(TimeoutException.class, e -> {
                LOGGER.error("{}  Timout during file seek {}", reqctx, path);
            });
        });
    }

    static int readExact(SeekableByteChannel chn, ByteBuffer buf) throws IOException {
        while (buf.remaining() > 0) {
            int n1 = chn.read(buf);
            if (n1 <= 0) {
                throw new IOException("can not read requested number of bytes");
            }
        }
        buf.flip();
        return 0;
    }

    public static Mono<PositionedDatafile> openAndPositionNoIndex(ReqCtx reqCtx, Path path, long beginNano, int fileno, int split) {
        return Mono.defer(() -> {
            try {
                long fileSize = Files.size(path);
                SeekableByteChannel chn = Files.newByteChannel(path, StandardOpenOption.READ);
                ByteBuffer buf = ByteBuffer.allocate(1024 * 8);
                buf.clear();
                buf.limit(6);
                readExact(chn, buf);
                if (buf.getShort() != 0) {
                    return Mono.error(new RuntimeException(String.format("corrupt file  %s", path)));
                }
                int j2 = buf.getInt();
                if (j2 < 1 || j2 > 256) {
                    return Mono.error(new RuntimeException("bad channelname in datafile"));
                }
                buf.clear();
                buf.limit(j2 - 8);
                readExact(chn, buf);
                {
                    CharBuffer s1 = StandardCharsets.UTF_8.decode(buf);
                    //String s1 = BaseEncoding.base16().lowerCase().decode(buf.array(), buf.position(), buf.remaining());
                    LOGGER.debug("{}  channel {}  path {}", reqCtx, s1, path);
                }
                buf.clear();
                buf.limit(4);
                readExact(chn, buf);
                int j2b = buf.getInt();
                if (j2b != j2) {
                    return Mono.error(new RuntimeException(String.format("len mismatch  %d  vs  %d", j2b, j2)));
                }

                int posBegin = 2 + j2;
                buf.clear();
                buf.limit(4);
                readExact(chn, buf);
                LOGGER.debug("{}  buffer  pos {}  lim {}  rem {}", reqCtx, buf.position(), buf.limit(), buf.remaining());
                int blobLen = buf.getInt();
                LOGGER.debug("{}  blobLen {}", reqCtx, blobLen);
                if (blobLen < 0 || blobLen > 6000) {
                    return Mono.error(new RuntimeException(String.format("bad blobLen  %d", blobLen)));
                }
                if (fileSize < posBegin + blobLen) {
                    return Mono.error(new RuntimeException("not a single blob in file"));
                }
                buf.clear();
                buf.limit(blobLen - 4);
                readExact(chn, buf);
                buf.getLong();
                long tsMin = buf.getLong();
                long pulse = buf.getLong();
                buf.getLong();
                buf.get();
                buf.get();
                LOGGER.debug("{}  1st event  ts {}  pulse {}", reqCtx, tsMin, pulse);
                {
                    int opt = buf.getInt();
                    if (opt < -1 || opt > 1000) {
                        LOGGER.error("{}  optional field len: {}", reqCtx, opt);
                        return Mono.error(new RuntimeException(String.format("optional field len: %d", opt)));
                    }
                    else if (opt > 0) {
                        buf.position(buf.position() + opt);
                    }
                }
                int mask = buf.get();
                boolean isCompressed = (mask & 0x80) != 0;
                boolean isArray = (mask & 0x40) != 0;
                boolean isShaped = (mask & 0x10) != 0;
                if (isCompressed) {
                    String s1 = BaseEncoding.base16().lowerCase().encode(buf.array(), 0, blobLen);
                    LOGGER.error("{}  compressed data in file without index  isCompressed {}  isArray {}  isShaped {}  mask {}  path {}\n{}", reqCtx, isCompressed, isArray, isShaped, mask, path, s1);
                    return Mono.error(new RuntimeException(String.format("Array data in file without index  mask %02x  path %s", mask, path)));
                }
                if (isArray) {
                    String s1 = BaseEncoding.base16().lowerCase().encode(buf.array(), 0, blobLen);
                    LOGGER.warn("{}  array data in file without index  isCompressed {}  isArray {}  isShaped {}  mask {}  path {}\n{}", reqCtx, isCompressed, isArray, isShaped, mask, path, s1);
                }
                if (tsMin >= beginNano) {
                    LOGGER.debug("{}  Start file at posBegin {}", reqCtx, posBegin);
                    chn.position(posBegin);
                    return Mono.just(PositionedDatafile.fromChannel(chn, path, fileno));
                }
                long posLast = (((fileSize - posBegin) / blobLen) - 1) * blobLen + posBegin;
                if (posLast < posBegin) {
                    return Mono.error(new RuntimeException("bad file structure"));
                }
                if (posLast == posBegin) {
                    chn.position(fileSize);
                    return Mono.just(PositionedDatafile.fromChannel(chn, path, fileno));
                }
                chn.position(posLast);
                buf.clear();
                buf.limit(blobLen);
                readExact(chn, buf);
                if (buf.remaining() != blobLen) {
                    return Mono.error(new RuntimeException(String.format("can not read a full blob  %d  vs  %d", buf.remaining(), blobLen)));
                }
                int blobLenB1 = buf.getInt(0);
                if (blobLenB1 != blobLen) {
                    return Mono.error(new RuntimeException(String.format("invalid blob len encountered  %d  vs  %d    posLast %d   path %s", blobLenB1, blobLen, posLast, path)));
                }
                long tsMax = buf.getLong(12);
                if (tsMax < beginNano) {
                    LOGGER.warn("{}  tsMax < beginNano   {} < {}", reqCtx, tsMax, beginNano);
                    chn.position(fileSize);
                    return Mono.just(PositionedDatafile.fromChannel(chn, path, fileno));
                }
                int btreads = 0;
                long j = posBegin;
                long k = posLast + blobLen;
                long tsFound = -1;
                long pulseFound = -1;
                while (k - j >= 2 * blobLen) {
                    long m = j + (k - j) / blobLen / 2 * blobLen;
                    chn.position(m);
                    buf.clear();
                    buf.limit(blobLen);
                    readExact(chn, buf);
                    btreads += 1;
                    if (buf.remaining() != blobLen) {
                        return Mono.error(new RuntimeException("can not read a full blob"));
                    }
                    if (buf.getInt(0) != blobLen) {
                        return Mono.error(new RuntimeException(String.format("%s  blobLen varies  path %s  %d  %d", reqCtx, path, blobLen, buf.getInt(0))));
                    }
                    long ts = buf.getLong(12);
                    if (ts >= beginNano) {
                        k = m;
                        tsFound = ts;
                        pulseFound = buf.getLong(20);
                    }
                    else {
                        j = m;
                    }
                }
                LOGGER.debug("{}  positioned file after  btreads {}  tsFound {}  pulseFound {}  k {}", reqCtx, btreads, tsFound, pulseFound, k);
                chn.position(k);
                LOGGER.debug("{}  positioned  noix  beginNano {}  fileno {}  split {}  path {}  pos {}  isArray {}", reqCtx, beginNano, fileno, split, path, k, isArray);
                return Mono.just(PositionedDatafile.fromChannel(chn, path, fileno));
            }
            catch (IOException e) {
                return Mono.error(e);
            }
        })
        .subscribeOn(Schedulers.boundedElastic());
    }

    public static Mono<List<Flux<PositionedDatafile>>> positionedDatafilesFromKeyspace(ReqCtx reqctx, KeyspaceOrder2 ksp, Instant begin, Instant end, List<Integer> splits) {
        long beginNanos = begin.toEpochMilli() * 1000000L;
        long endNanos = end.toEpochMilli() * 1000000L;
        Mono<List<Flux<PositionedDatafile>>> ret = Flux.fromIterable(ksp.splits)
        .filter(x -> splits.isEmpty() || splits.contains(x.split))
        .map(sp -> {
            BaseDir base = ksp.channel.base;
            // TODO need to sort by true start?
            return Flux.fromIterable(sp.timeBins)
            .map(tb -> {
                return Tuples.of(
                tb,
                String.format("%s/%s_%s/byTime/%s/%019d/%010d/%019d_%05d_Data", base.baseDir, base.baseKeyspaceName, ksp.ksp, ksp.channel.name, tb.timeBin, sp.split, tb.binSize, 0)
                );
            })
            .map(x -> Tuples.of(x.getT1(), Path.of(x.getT2())))
            .index()
            .concatMap(x -> {
                int fileId = x.getT1().intValue();
                Path path = x.getT2().getT2();
                if (x.getT2().getT1().hasIndex) {
                    return PositionedDatafile.openAndPosition(reqctx, path, beginNanos);
                }
                else {
                    return PositionedDatafile.openAndPositionNoIndex(reqctx, path, beginNanos, fileId, sp.split);
                }
            }, 1);
        })
        .collectList();
        return ret;
    }

    public SeekableByteChannel takeChannel() {
        SeekableByteChannel c = channel;
        channel = null;
        return c;
    }

    public void release() {
        if (channel != null) {
            try {
                channel.close();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            channel = null;
        }
    }

}
