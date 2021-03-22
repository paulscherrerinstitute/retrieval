package ch.psi.daq.retrieval.subnodes;

import ch.psi.daq.retrieval.bytes.BufCont;
import ch.psi.daq.retrieval.config.Node;
import ch.psi.daq.retrieval.controller.QueryData;
import ch.psi.daq.retrieval.pod.api1.Query;
import ch.psi.daq.retrieval.reqctx.ReqCtx;
import ch.psi.daq.retrieval.utils.PubRepeat;
import ch.psi.daq.retrieval.utils.Tools;
import ch.qos.logback.classic.Logger;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.NettyDataBufferFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class RawClient {
    static final Logger LOGGER = (Logger) LoggerFactory.getLogger(RawClient.class.getSimpleName());

    public static class Opts {
        public static Opts create() {
            return new Opts();
        }
        public Opts wgroup(int k) {
            wgroup = k;
            return this;
        }
        int wgroup;
    }

    public static Mono<SubStream> rawClient(ReqCtx reqCtx, Query subq, String channelName, Node node, Opts opts) {
        LOGGER.debug("{}  rawClient prepare to open  {}", reqCtx, node.host);
        SubStream ret = new SubStream();
        ret.reqId = String.format("prelim-RawClient-%s-%s", node.host, channelName);
        ret.fl = subFlux(reqCtx, subq, channelName, node, opts, ret)
        .map(k -> BufCont.fromBuffer(nfac.wrap(k), BufCont.Mark.RawCl1))
        .doOnNext(q -> q.appendMark(BufCont.Mark.RawCl2))
        .transform(QueryData.doDiscard("RawClPub"));
        return Mono.just(ret);
    }

    public static Flux<ByteBuf> subFlux(ReqCtx reqCtx, Query subq, String channelName, Node node, Opts opts, SubStream subStream) {
        RawClient client = new RawClient(reqCtx, subq, channelName, node, subStream);
        return Flux.create(sink -> client.create(sink, opts));
    }

    public RawClient(ReqCtx reqCtx, Query subq, String channelName, Node node, SubStream subStream) {
        this.reqCtx = reqCtx;
        this.subq = subq;
        this.channelName = channelName;
        this.node = node;
        this.subStream = subStream;
        this.watchdog = new Thread(this::watchdog, String.format("RCWd-%d", ctor.getAndAdd(1)));
        this.watchdog.start();
    }

    public void create(FluxSink<ByteBuf> sink, Opts opts) {
        sinkRef.set(sink);
        sink.onCancel(this::onCancel);
        sink.onDispose(this::onDispose);
        sink.onRequest(this::onRequest);
        fetch(opts.wgroup);
    }

    void onCancel() {
        didCancel.set(1);
        ChannelHandlerContext ctx = ctxRef.get();
        if (ctx != null) {
            LOGGER.debug("{}  sink cancel  CLOSE NETTY  {}  {}", reqCtx, node, channelName);
            ctx.close();
        }
        else {
            LOGGER.info("{}  sink cancel  NO CTX  {}  {}", reqCtx, node, channelName);
        }
    }

    void onDispose() {
        didDispose.set(1);
        LOGGER.debug("{}  sink dispose from downstream  {}  {}", reqCtx, node, channelName);
    }

    void onRequest(long n) {
        requestedCount.getAndAdd(1);
        if (n < 1) {
            LOGGER.error("{}  invalid request  {}  {}  {}  {}", reqCtx, n, requested.get(), node, channelName);
            return;
        }
        if (n > 20000) {
            LOGGER.error("{}  too much request  {}  {}  {}  {}", reqCtx, n, requested.get(), node, channelName);
            return;
        }
        if (firstReadDecided.get() == 0) {
            synchronized (ctxRef) {
                long r = requested.getAndAdd(n);
                if (firstReadDecided.get() == 0) {
                    ChannelHandlerContext ctx = ctxRef.get();
                    if (ctx != null) {
                        requestFirstRead.getAndAdd(1);
                        firstReadDecided.set(1);
                        ctx.read();
                    }
                    else {
                        requestNoFirstRead.getAndAdd(1);
                    }
                }
                else {
                    if (r <= 0) {
                        ctxRef.get().read();
                    }
                }
            }
        }
        else {
            long r = requested.getAndAdd(n);
            if (r <= 0) {
                ctxRef.get().read();
            }
        }
    }

    public void fetch(int wgroup) {
        try {
            Bootstrap b = new Bootstrap();
            if (wgroup == 1) {
                b.group(workerGroup2);
            }
            else if (wgroup == 2) {
                b.group(workerGroup3);
            }
            else {
                b.group(workerGroup1);
            }
            b.channel(NioSocketChannel.class);
            b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 4000);
            b.option(ChannelOption.AUTO_READ, false);
            b.handler(new ChanInit(this));
            LOGGER.debug("{}  connecting  {}  {}", reqCtx, node.host, channelName);
            ChannelFuture f = b.connect(node.host, node.port + 100).sync();
            f.channel().closeFuture().addListener(k -> {
                channelCloseCount.getAndAdd(1);
            });
        }
        catch (InterruptedException e) {
            LOGGER.error("{}  fetch  {}  {}  {}", reqCtx, node.host, channelName, e);
            throw new RuntimeException(e);
        }
    }

    void watchdog() {
        wdCount.getAndAdd(1);
        while (sinkTerm.get() == 0 && didCancel.get() == 0) {
            Tools.sleep(2000);
            long g = requested.get();
            if (g > 0) {
                long dt = System.nanoTime() - tsSunkenLast.get();
                dt /= 1000000000L;
                if (dt > 5) {
                    wdSilenceCount.getAndAdd(1);
                    LOGGER.warn("{}  requested {}  but long time no buffer  {} s  {}  {}", reqCtx, g, dt, node, channelName);
                }
            }
        }
        wdCount.getAndAdd(-1);
    }

    public static class Stats {
        public long channelActiveCount;
        public long channelInactiveCount;
        public long channelCloseCount;
        public long channelExceptionCount;
        public long channelReadCount;
        public long sinkRequestCount;
        public long sinkRequestItems;
        public long sunkBytes;
        public long requestFirstRead;
        public long requestNoFirstRead;
        public long handlerFirstRead;
        public long handlerNoFirstRead;
        public long handlerActiveLoopTrue;
        public long handlerActiveLoopFalse;
        public long handlerIdleRead;
        public long handlerIdleWrite;
        public long handlerIdleAll;
        public long readTimeout;
        public long wdCount;
        public long wdSilenceCount;
        public Stats() {
            channelActiveCount = RawClient.channelActiveCount.get();
            channelInactiveCount = RawClient.channelInactiveCount.get();
            channelCloseCount = RawClient.channelCloseCount.get();
            channelExceptionCount = RawClient.channelExceptionCount.get();
            channelReadCount = RawClient.channelReadCount.get();
            sinkRequestCount = RawClient.sinkRequestCount.get();
            sinkRequestItems = RawClient.sinkRequestItems.get();
            sunkBytes = RawClient.sunkBytes.get();
            requestFirstRead = RawClient.requestFirstRead.get();
            requestNoFirstRead = RawClient.requestNoFirstRead.get();
            handlerFirstRead = RawClient.handlerFirstRead.get();
            handlerNoFirstRead = RawClient.handlerNoFirstRead.get();
            handlerActiveLoopTrue = RawClient.handlerActiveLoopTrue.get();
            handlerActiveLoopFalse = RawClient.handlerActiveLoopFalse.get();
            handlerIdleRead = RawClient.handlerIdleRead.get();
            handlerIdleWrite = RawClient.handlerIdleWrite.get();
            handlerIdleAll = RawClient.handlerIdleAll.get();
            readTimeout = RawClient.readTimeout.get();
            wdCount = RawClient.wdCount.get();
            wdSilenceCount = RawClient.wdSilenceCount.get();
        }
    }

    static Scheduler rawclpub = Schedulers.newBoundedElastic(64, 128, "rawclpub");
    static final NettyDataBufferFactory nfac = new NettyDataBufferFactory(UnpooledByteBufAllocator.DEFAULT);
    static final EventLoopGroup workerGroup1 = new NioEventLoopGroup(1);
    static final EventLoopGroup workerGroup2 = new NioEventLoopGroup(4);
    static final EventLoopGroup workerGroup3 = new NioEventLoopGroup(16);
    static final AtomicLong channelActiveCount = new AtomicLong();
    static final AtomicLong channelInactiveCount = new AtomicLong();
    static final AtomicLong channelCloseCount = new AtomicLong();
    static final AtomicLong channelExceptionCount = new AtomicLong();
    static final AtomicLong channelReadCount = new AtomicLong();
    static final AtomicLong sinkRequestCount = new AtomicLong();
    static final AtomicLong sinkRequestItems = new AtomicLong();
    static final AtomicLong sunkBytes = new AtomicLong();
    static final AtomicLong requestFirstRead = new AtomicLong();
    static final AtomicLong requestNoFirstRead = new AtomicLong();
    static final AtomicLong handlerFirstRead = new AtomicLong();
    static final AtomicLong handlerNoFirstRead = new AtomicLong();
    static final AtomicLong handlerActiveLoopTrue = new AtomicLong();
    static final AtomicLong handlerActiveLoopFalse = new AtomicLong();
    static final AtomicLong handlerIdleRead = new AtomicLong();
    static final AtomicLong handlerIdleWrite = new AtomicLong();
    static final AtomicLong handlerIdleAll = new AtomicLong();
    static final AtomicLong readTimeout = new AtomicLong();
    static final AtomicLong wdCount = new AtomicLong();
    static final AtomicLong wdSilenceCount = new AtomicLong();
    static final AtomicLong ctor = new AtomicLong();
    volatile int readGuide;
    final AtomicInteger firstReadDecided = new AtomicInteger();
    final AtomicInteger sinkTerm = new AtomicInteger();
    final AtomicInteger didCancel = new AtomicInteger();
    final AtomicInteger didDispose = new AtomicInteger();
    final AtomicLong requestedCount = new AtomicLong();
    final AtomicLong requested = new AtomicLong();
    final AtomicLong tsSunkenLast = new AtomicLong();
    final AtomicReference<FluxSink<ByteBuf>> sinkRef = new AtomicReference<>();
    final AtomicReference<ChannelHandlerContext> ctxRef = new AtomicReference<>();
    final ReqCtx reqCtx;
    final Query subq;
    final String channelName;
    final Node node;
    final SubStream subStream;
    final Thread watchdog;
    final AtomicLong tsReq = new AtomicLong();

}