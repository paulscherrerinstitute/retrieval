package ch.psi.daq.retrieval.utils;

import ch.psi.daq.retrieval.QueryParams;
import ch.psi.daq.retrieval.bytes.BufCont;
import ch.psi.daq.retrieval.finder.BaseDirFinderFormatV0;
import ch.psi.daq.retrieval.finder.ScanDataFilesResult;
import ch.psi.daq.retrieval.finder.TimeBin2;
import ch.psi.daq.retrieval.reqctx.BufCtx;
import ch.psi.daq.retrieval.reqctx.ReqCtx;
import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

public class ChannelEventStream {
    static final Logger LOGGER = (Logger) LoggerFactory.getLogger(ChannelEventStream.class.getSimpleName());
    static final AtomicLong diskBufferCount = new AtomicLong();
    static final AtomicLong diskBufferBytes = new AtomicLong();
    static final AtomicLong discardCount = new AtomicLong();
    static final AtomicLong discardDataBufferCount = new AtomicLong();
    static final AtomicLong discardBytes = new AtomicLong();

    public static class Stats {
        public long discardCount;
        public long discardBytes;
        public long discardDataBufferCount;
        public long diskBufferCount;
        public long diskBufferBytes;
        public Stats() {
            discardCount = ChannelEventStream.discardCount.get();
            discardBytes = ChannelEventStream.discardBytes.get();
            discardDataBufferCount = ChannelEventStream.discardDataBufferCount.get();
            diskBufferCount = ChannelEventStream.diskBufferCount.get();
            diskBufferBytes = ChannelEventStream.diskBufferBytes.get();
        }
    }

    public static Mono<SplitDataFlux> channelDataFluxes(ReqCtx reqCtx, QueryParams qp, BaseDirFinderFormatV0 baseDirFinder, Instant beg, Instant end, Duration waitForData, String channelName, int split) {
        long begNanos = DateExt.toLong(beg);
        long endNanos = DateExt.toLong(end);
        return baseDirFinder.findMatchingDataFiles(reqCtx, channelName, beg, end, split)
        .map(res -> {
            Flux<BufCont> fl = res.timeBins
            .concatMap(tb -> {
                return dataFluxForTimebin(reqCtx, qp, res, tb, beg, begNanos, endNanos, waitForData);
            }, 0);
            return new SplitDataFlux(res.channelConfigEntry, split, fl);
        })
        .subscribeOn(fs);
    }

    static class ChanStats {
        int capMax;
    }

    static Flux<BufCont> dataFluxForTimebin(ReqCtx reqCtx, QueryParams qp, ScanDataFilesResult scf, TimeBin2 tb, Instant beg, long beginNanos, long endNanos, Duration waitForData) {
        Path path = scf.dataPath(tb);
        //LOGGER.info("{}  dataFluxForTimebin  path {}", reqCtx, path);
        int attempts;
        if (waitForData.isZero()) {
            attempts = 1;
        }
        else {
            attempts = 60 * 10 / 2;
        }
        ChanStats chanStats = new ChanStats();
        return Flux.range(0, attempts)
        .concatMap(i1 -> {
            if (i1 > 0) {
                LOGGER.info("{}  next attempt  delay  {}", reqCtx, i1);
                return Mono.just(i1).delayElement(Duration.ofMillis(2000));
            }
            else {
                return Mono.just(i1);
            }
        }, 0)
        .takeUntil(i1 -> {
            if (i1 > 0) {
                if (Instant.now().isAfter(beg.plus(waitForData))) {
                    LOGGER.info("{}  open attempt {}  tb {}  {}   TIMEOUT", reqCtx, i1, tb.timeBin, scf.locatedChannel);
                    return true;
                }
                else {
                    LOGGER.info("{}  open attempt {}  tb {}  {}", reqCtx, i1, tb.timeBin, scf.locatedChannel);
                    return false;
                }
            }
            else {
                return false;
            }
        })
        .concatMap(i1 -> {
            return PositionedDatafile.openAndPosition(reqCtx, path, beginNanos)
            .onErrorResume(e -> PositionedDatafile.openAndPositionNoIndex(reqCtx, path, beginNanos));
        }, 0)
        .onErrorResume(e -> {
            return Mono.empty();
        })
        .take(1)
        .onErrorResume(e -> {
            LOGGER.error("{}  while creating PositionedDatafile  (again)  {}\n{}", reqCtx, e.toString(), Tools.formatTrace(e));
            return Mono.empty();
        })
        .concatMap(pd -> {
            BufCtx bufCtx = reqCtx.bufCtx;
            SeekableByteChannel ch = pd.takeChannel();
            AtomicLong posChBeg = new AtomicLong();
            AtomicLong posChLast = new AtomicLong();
            return Mono.defer(() -> {
                try {
                    return Mono.just(ch.position());
                }
                catch (IOException e) {
                    return Mono.error(e);
                }
            })
            .flatMapMany(pos -> {
                posChBeg.set(pos);
                posChLast.set(pos);
                AtomicLong byteCount = new AtomicLong();
                return Flux.range(0, attempts)
                .concatMap(i1 -> {
                    if (i1 > 0) {
                        LOGGER.info("{}  repeated read attempt {}", reqCtx, i1);
                        return Mono.just(i1).delayElement(Duration.ofMillis(500));
                    }
                    else {
                        return Mono.just(i1);
                    }
                }, 0)
                .takeUntil(i1 -> {
                    if (i1 > 0) {
                        if (Instant.now().isAfter(beg.plus(waitForData))) {
                            LOGGER.info("{}  try read more data  attempt {}  tb {}  {}  TIMEOUT", reqCtx, i1, tb.timeBin, scf.locatedChannel);
                            return true;
                        }
                        else {
                            LOGGER.info("{}  try read more data  attempt {}  tb {}  {}", reqCtx, i1, tb.timeBin, scf.locatedChannel);
                            return false;
                        }
                    }
                    else {
                        return false;
                    }
                })
                .concatMap(i1 -> {
                    //LOGGER.info("{}  op ReadGenerator for {}", reqCtx, path);
                    //Flux.using(ch, channel -> Flux.generate(new ReadGenerator(channel, reqCtx.bufCtx.bufFac, reqCtx.bufCtx.bufferSize)), ChannelEventStream::closeChannel);
                    return Flux.generate(new ReadGenerator(ch, bufCtx.bufFac, qp.bufferSizeDiskRead));
                }, 0)
                .doFinally(k -> {
                    closeChannel(ch);
                })
                .takeWhile(buf -> {
                    int rb = buf.readableByteCount();
                    chanStats.capMax = Math.max(chanStats.capMax, rb);
                    byteCount.getAndAdd(rb);
                    long z = byteCount.get();
                    if (qp.limitRawRead == 0 || z < qp.limitRawRead) {
                        return true;
                    }
                    else {
                        DataBufferUtils.release(buf);
                        return false;
                    }
                })
                .doFinally(k -> {
                    reqCtx.requestStatus().seenBufCapMax(chanStats.capMax);
                });
            })
            .map(buf -> {
                diskBufferCount.getAndAdd(1);
                diskBufferBytes.getAndAdd(buf.readableByteCount());
                return BufCont.fromBuffer(buf, BufCont.Mark.ChEvS_buf);
            })
            .doOnDiscard(DataBuffer.class, obj -> {
                discardDataBufferCount.getAndAdd(1);
                DataBufferUtils.release(obj);
            })
            .doOnDiscard(BufCont.class, buf -> {
                discardCount.getAndAdd(1);
                if (buf.hasBuf()) {
                    DataBuffer b2 = buf.bufferRef();
                    discardBytes.getAndAdd(b2.capacity());
                }
                buf.close();
            });
        }, 0)
        //.doOnSubscribe(scr -> LOGGER.info("{}  get subscribed to {}", reqCtx, path))
        ;
    }

    static void closeChannel(SeekableByteChannel ch) {
        if (ch != null && ch.isOpen()) {
            try {
                ch.close();
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    static Scheduler fs = Schedulers.newBoundedElastic(64, 512, "fs");

}
