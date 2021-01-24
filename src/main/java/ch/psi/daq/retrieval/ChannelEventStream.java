package ch.psi.daq.retrieval;

import ch.psi.daq.retrieval.bytes.BufCont;
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class ChannelEventStream {
    static final Logger LOGGER = LoggerFactory.getLogger(ChannelEventStream.class.getSimpleName());
    final DataBufferFactory bufFac;
    static final AtomicLong diskBufferCount = new AtomicLong();
    static final AtomicLong diskBufferBytes = new AtomicLong();
    static final AtomicLong discardCount = new AtomicLong();
    static final AtomicLong discardBytes = new AtomicLong();

    public static class Stats {
        public long discardCount;
        public long discardBytes;
        public long diskBufferCount;
        public long diskBufferBytes;
        public Stats() {
            discardCount = ChannelEventStream.discardCount.get();
            discardBytes = ChannelEventStream.discardBytes.get();
            diskBufferCount = ChannelEventStream.diskBufferCount.get();
            diskBufferBytes = ChannelEventStream.diskBufferBytes.get();
        }
    }

    public ChannelEventStream(DataBufferFactory bufFac) {
        this.bufFac = bufFac;
    }

    public static <T> Mono<List<Flux<T>>> dataFluxFromFiles(KeyspaceToDataParams toDataParams, MapFunctionFactory<T> transFac, BufCtx bufctx) {
        ReqCtx reqCtx = toDataParams.reqctx;
        long beginNanos = toDataParams.begin.toEpochMilli() * 1000000L;
        return Flux.fromIterable(toDataParams.ksp.splits)
        .filter(x -> toDataParams.splits.isEmpty() || toDataParams.splits.contains(x.split))
        .map(sp -> {
            LOGGER.debug("{}  open split {}", reqCtx, sp.split);
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
                    LOGGER.debug("{}  open datafile  {}  {}  {}", reqCtx, x.getT1(), fsize, path);
                    if (x.getT2().getT1().hasIndex) {
                        reqCtx.channelFileFound(new Channel(ksp.channel.base, ksp.channel.name), path);
                        return Optional.of(PositionedDatafile.openAndPosition(reqCtx, path, beginNanos, fileno, split));
                    }
                    else {
                        reqCtx.channelFileFound(new Channel(ksp.channel.base, ksp.channel.name), path);
                        return Optional.of(PositionedDatafile.openAndPositionNoIndex(reqCtx, path, beginNanos, fileno, split));
                    }
                }
                catch (IOException e) {
                    LOGGER.debug("{}  datafile not present  {}  {}", reqCtx, x.getT1(), path);
                    reqCtx.channelFileNotFound(new Channel(ksp.channel.base, ksp.channel.name), path);
                    return Optional.<Mono<PositionedDatafile>>empty();
                }
            })
            .doOnNext(x -> {
                LOGGER.debug("{}  dataFluxFromFiles  present {}  channel {}  split {}", reqCtx, x.isPresent(), ksp.channel.name, sp.split);
            })
            .filter(Optional::isPresent)
            .concatMap(Optional::get)
            .map(Optional::of)
            .onErrorResume(e -> {
                LOGGER.error("{}  while creating PositionedDatafile  {}", reqCtx, e.toString());
                ByteArrayOutputStream ba = new ByteArrayOutputStream();
                PrintStream ps = new PrintStream(ba);
                ps.println(e.toString());
                Arrays.stream(e.getStackTrace()).limit(100).forEach(k -> ps.println(k.toString()));
                ps.close();
                LOGGER.error("{}  while creating PositionedDatafile  (again)  {}\n{}", reqCtx, e.toString(), ba.toString(StandardCharsets.UTF_8));
                return Mono.just(Optional.empty());
            })
            .filter(Optional::isPresent)
            .map(Optional::get)
            .concatMap(f -> {
                LOGGER.debug("{}  read byte channel with buffersize {}  fileno {}", reqCtx, toDataParams.bufferSize, f.fileno);
                Flux<BufCont> fl = DataBufferUtils.readByteChannel(f::takeChannel, toDataParams.bufFac, toDataParams.bufferSize)
                /*
                .map(k -> {
                    ByteBuffer bb = ByteBuffer.allocate(k.readableByteCount());
                    bb.position(0);
                    bb.limit(k.readableByteCount());
                    k.read(bb.array(), bb.arrayOffset(), k.readableByteCount());
                    DataBufferUtils.release(k);
                    return reqCtx.bufCtx.bufFac.wrap(bb);
                })
                */
                .doOnDiscard(DataBuffer.class, DataBufferUtils::release)
                .map(buf -> {
                    diskBufferCount.getAndAdd(1);
                    diskBufferBytes.getAndAdd(buf.readableByteCount());
                    return BufCont.fromBuffer(buf, BufCont.Mark.ChEvS_buf);
                })
                .doOnDiscard(BufCont.class, buf -> {
                    discardCount.getAndAdd(1);
                    if (buf.hasBuf()) {
                        DataBuffer b2 = buf.bufferRef();
                        discardBytes.getAndAdd(b2.capacity());
                    }
                    buf.close();
                });
                return transFac.makeTrans(fl, toDataParams, f.fileno);
            }, 1);
        })
        .collectList();
    }


    /*
    public static <T> Mono<List<Flux<T>>> dataFluxFromFiles2(KeyspaceToDataParams toDataParams, MapFunctionFactory<T> transFac, BufCtx bufctx) {
        ReqCtx reqCtx = toDataParams.reqctx;
        long beginNanos = toDataParams.begin.toEpochMilli() * 1000000L;
        return Flux.fromIterable(toDataParams.ksp.splits)
        .filter(x -> toDataParams.splits.isEmpty() || toDataParams.splits.contains(x.split))
        .map(sp -> {
            LOGGER.debug("{}  open split {}", reqCtx, sp.split);
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
            .concatMap(xk -> {
                return Mono.fromCallable(() -> {
                    int fileno = (int) (long) xk.getT1();
                    int split = xk.getT2().getT3();
                    Path path = xk.getT2().getT2();
                    try {
                        long fsize = Files.size(path);
                        LOGGER.debug("{}  open datafile  {}  {}  {}", reqCtx, xk.getT1(), fsize, path);
                        if (xk.getT2().getT1().hasIndex) {
                            reqCtx.channelFileFound(new Channel(ksp.channel.base, ksp.channel.name), path);
                            return Optional.of(PositionedDatafile.openAndPosition(reqCtx, path, beginNanos, fileno, split));
                        }
                        else {
                            reqCtx.channelFileFound(new Channel(ksp.channel.base, ksp.channel.name), path);
                            return Optional.of(PositionedDatafile.openAndPositionNoIndex(reqCtx, path, beginNanos, fileno, split));
                        }
                    }
                    catch (IOException e) {
                        LOGGER.debug("{}  datafile not present  {}  {}", reqCtx, xk.getT1(), path);
                        reqCtx.channelFileNotFound(new Channel(ksp.channel.base, ksp.channel.name), path);
                        return Optional.<Mono<PositionedDatafile>>empty();
                    }
                })
                .doOnNext(x -> {
                    LOGGER.debug("{}  dataFluxFromFiles  present {}  channel {}  split {}", reqCtx, x.isPresent(), ksp.channel.name, sp.split);
                })
                .filter(Optional::isPresent)
                .flatMap(Optional::get)
                .map(Optional::of)
                .onErrorResume(e -> {
                    LOGGER.error("{}  while creating PositionedDatafile  {}", reqCtx, e.toString());
                    ByteArrayOutputStream ba = new ByteArrayOutputStream();
                    PrintStream ps = new PrintStream(ba);
                    ps.println(e.toString());
                    Arrays.stream(e.getStackTrace()).limit(100).forEach(k -> ps.println(k.toString()));
                    ps.close();
                    LOGGER.error("{}  while creating PositionedDatafile  (again)  {}\n{}", reqCtx, e.toString(), ba.toString(StandardCharsets.UTF_8));
                    return Mono.just(Optional.empty());
                })
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(f -> {
                    LOGGER.debug("{}  read byte channel with buffersize {}  fileno {}", reqCtx, bufctx.bufferSize, f.fileno);
                    Flux<BufCont> fl = DataBufferUtils.readByteChannel(f::takeChannel, bufctx.bufFac, bufctx.bufferSize)
                    .doOnDiscard(DataBuffer.class, DataBufferUtils::release)
                    .map(buf -> {
                        diskBufferCount.getAndAdd(1);
                        diskBufferBytes.getAndAdd(buf.readableByteCount());
                        return BufCont.fromBuffer(buf, BufCont.Mark.ChEvS_buf);
                    })
                    .doOnDiscard(BufCont.class, buf -> {
                        discardCount.getAndAdd(1);
                        if (buf.hasBuf()) {
                            DataBuffer b2 = buf.bufferRef();
                            discardBytes.getAndAdd(b2.capacity());
                        }
                        buf.close();
                    });
                    return transFac.makeTrans(fl, toDataParams, f.fileno);
                });
            }, 1);
        })
        .collectList();
    }
    */

}
