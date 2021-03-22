package ch.psi.daq.retrieval.utils;

import ch.psi.daq.retrieval.finder.BaseDir;
import ch.psi.daq.retrieval.finder.KeyspaceOrder2;
import ch.psi.daq.retrieval.reqctx.ReqCtx;
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

    PositionedDatafile(SeekableByteChannel channel, Path path) {
        this.path = path;
        this.channel = channel;
    }

    public static PositionedDatafile fromChannel(SeekableByteChannel channel, Path path) {
        return new PositionedDatafile(channel, path);
    }

    public static Mono<PositionedDatafile> openAndPosition(ReqCtx reqctx, Path path, long beginNano) {
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
                LOGGER.debug("{}  Seek  position {} / {}   path {}", reqctx, v, e, path);
                c.position(v);
                LOGGER.debug("{}  positioned    ix  beginNano {}  pos {}  path {}", reqctx, beginNano, v, path);
                return PositionedDatafile.fromChannel(c, path);
            })
            .timeout(Duration.ofMillis(4000))
            .doOnError(TimeoutException.class, e -> {
                LOGGER.error("{}  Timout during file seek {}", reqctx, path);
            });
        });
    }

    public static Mono<PositionedDatafile> openAndPositionNoIndex(ReqCtx reqCtx, Path path, long beginNano) {
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
                    return Mono.error(new PositionImpossibleWithoutIndex(String.format("Array data in file without index  mask %02x  path %s", mask, path)));
                }
                if (isArray) {
                    String s1 = BaseEncoding.base16().lowerCase().encode(buf.array(), 0, blobLen);
                    LOGGER.warn("{}  array data in file without index  isCompressed {}  isArray {}  isShaped {}  mask {}  path {}\n{}", reqCtx, isCompressed, isArray, isShaped, mask, path, s1);
                }
                if (tsMin >= beginNano) {
                    LOGGER.debug("{}  Start file at posBegin {}", reqCtx, posBegin);
                    chn.position(posBegin);
                    return Mono.just(PositionedDatafile.fromChannel(chn, path));
                }
                long posLast = (((fileSize - posBegin) / blobLen) - 1) * blobLen + posBegin;
                if (posLast < posBegin) {
                    return Mono.error(new RuntimeException("bad file structure"));
                }
                if (posLast == posBegin) {
                    chn.position(fileSize);
                    return Mono.just(PositionedDatafile.fromChannel(chn, path));
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
                    return Mono.just(PositionedDatafile.fromChannel(chn, path));
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
                    int len3 = buf.getInt(0);
                    if (len3 != blobLen) {
                        return Mono.error(new RuntimeException(String.format("%s  invalid blob len during search  %d vs %d  %s", reqCtx, len3, blobLen, path)));
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
                LOGGER.debug("{}  positioned  noix  beginNano {}  path {}  pos {}  isArray {}", reqCtx, beginNano, path, k, isArray);
                return Mono.just(PositionedDatafile.fromChannel(chn, path));
            }
            catch (IOException e) {
                return Mono.error(e);
            }
        })
        .subscribeOn(Schedulers.boundedElastic());
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

    public SeekableByteChannel channel;
    public Path path;

}
