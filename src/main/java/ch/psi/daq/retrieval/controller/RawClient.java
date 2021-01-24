package ch.psi.daq.retrieval.controller;

import ch.psi.daq.retrieval.config.Node;
import ch.psi.daq.retrieval.pod.api1.Query;
import ch.qos.logback.classic.Logger;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.NettyDataBufferFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class RawClient {
    static final Logger LOGGER = (Logger) LoggerFactory.getLogger(RawClient.class.getSimpleName());

    public static class Stats {
        public long channelActiveCount;
        public long channelInactiveCount;
        public long channelCloseCount;
        public long channelExceptionCount;
        public long channelReadCount;
        public long sinkRequestCount;
        public long sinkRequestItems;
        public long sunkBytes;
        public Stats() {
            channelActiveCount = RawClient.channelActiveCount.get();
            channelInactiveCount = RawClient.channelInactiveCount.get();
            channelCloseCount = RawClient.channelCloseCount.get();
            channelExceptionCount = RawClient.channelExceptionCount.get();
            channelReadCount = RawClient.channelReadCount.get();
            sinkRequestCount = RawClient.sinkRequestCount.get();
            sinkRequestItems = RawClient.sinkRequestItems.get();
            sunkBytes = RawClient.sunkBytes.get();
        }
    }

    public static Mono<SubStream> rawSub(Query subq, String channelName, Node node) {
        SubStream ret = new SubStream();
        ret.reqId = String.format("RawClient-%s-%s", node.host, channelName);
        ret.fl = subFlux(subq, channelName, node)
        .map(nfac::wrap);
        return Mono.just(ret);
    }

    public static Flux<ByteBuf> subFlux(Query subq, String channelName, Node node) {
        RawClient client = new RawClient();
        client.subq = subq;
        client.channelName = channelName;
        client.node = node;
        return Flux.create(client::create);
    }

    public void create(FluxSink<ByteBuf> sink) {
        sinkRef.set(sink);
        sink.onCancel(() -> {
            LOGGER.info("sink cancel from downstream  {}  {}", node.host, channelName);
            didCancel.set(1);
        });
        sink.onDispose(() -> {
            LOGGER.debug("sink dispose from downstream  {}  {}", node.host, channelName);
            didDispose.set(1);
        });
        sink.onRequest(n -> {
            LOGGER.trace("request from downstream {}", n);
            if (requested.getAndAdd(n) <= 0) {
                ChannelHandlerContext ctx = ctxRef.get();
                if (ctx != null) {
                    ctx.read();
                }
            }
        });
        fetch();
        //new Thread(this::fetch).start();
    }

    public void fetch() {
        try {
            Bootstrap b = new Bootstrap();
            b.group(workerGroup);
            b.channel(NioSocketChannel.class);
            //b.option(ChannelOption.SO_KEEPALIVE, true);
            b.handler(new ChanInit(this));
            LOGGER.info("connecting  {}  {}", node.host, channelName);
            ChannelFuture f = b.connect(node.host, node.port + 100).sync();
            f.channel().closeFuture().addListener(k -> {
                channelCloseCount.getAndAdd(1);
                LOGGER.debug("channel closed  {}  {}  {}  {}", node.host, channelName, k.isSuccess(), k.cause());
            });
            //f.channel().closeFuture().sync();
            //LOGGER.info("connection closed  {}  {}", node.host, channelName);
        }
        catch (InterruptedException e) {
            LOGGER.error("fetch  {}  {}  {}", node.host, channelName, e);
            throw new RuntimeException(e);
        }
        finally {
            // TODO does each client need own worker group?
            //workerGroup.shutdownGracefully();
        }
        //LOGGER.info("fetch triggered  {}  {}", node.host, channelName);
    }

    static class ChanInit extends ChannelInitializer<SocketChannel> {
        ChanInit(RawClient client) {
            this.client = client;
        }
        @Override
        public void initChannel(SocketChannel ch) throws Exception {
            ch.pipeline().addLast(new Handler(client));
        }
        RawClient client;
    }

    static class Handler extends ChannelInboundHandlerAdapter {
        Handler(RawClient client) {
            this.client = client;
        }
        @Override
        public void channelActive(final ChannelHandlerContext ctx) {
            RawClient.channelActiveCount.getAndAdd(1);
            ctx.channel().config().setOption(ChannelOption.AUTO_READ, false);
            client.ctxRef.set(ctx);
            long nreq = client.requested.get();
            if (nreq > 0) {
                //LOGGER.info("channelActive  downstream already requested {}", nreq);
                ctx.read();
            }
            else {
                //LOGGER.info("channelActive  no downstream yet");
            }
            String js5;
            try {
                js5 = new ObjectMapper().writeValueAsString(client.subq);
            }
            catch (IOException e) {
                js5 = "error";
            }
            ByteBuffer bq = ByteBuffer.allocate(1024);
            ByteBuffer jsbuf = StandardCharsets.UTF_8.encode(js5);
            bq.putInt(jsbuf.remaining());
            bq.put(jsbuf);
            bq.flip();
            ByteBuf bbjs = Unpooled.wrappedBuffer(bq);
            LOGGER.info("sending request  to {}  size {}  {}", client.node.host, bbjs.readableBytes(), js5);
            ChannelFuture f = ctx.writeAndFlush(bbjs);
            /*
            f.addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    LOGGER.info("query outbound sent  {}  {}", client.node.host, client.channelName);
                    //ctx.close();
                }
            });
            */
        }
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object obj) {
            RawClient.channelReadCount.getAndAdd(1);
            while (queue.size() > 0 && client.requested.get() > 0) {
                if (client.requested.getAndAdd(-1) > 0) {
                    try {
                        client.sinkRef.get().next(queue.remove());
                    }
                    catch (NoSuchElementException e) {
                        break;
                    }
                }
                else {
                    client.requested.getAndAdd(1);
                }
            }
            if (!(obj instanceof ByteBuf)) {
                throw new RuntimeException("incoming data is not buffer");
            }
            ByteBuf buf = (ByteBuf) obj;
            if (client.didCancel.get() != 0) {
                buf.release();
                ctx.close();
            }
            else if (client.didDispose.get() != 0) {
                buf.release();
                ctx.close();
            }
            else {
                //LOGGER.info("channelRead  {}  {}  {}  req {}  sinkReq {}", buf.readableBytes(), client.node.host, client.channelName, client.requested.get(), client.sinkRef.get().requestedFromDownstream());
                if (client.requested.getAndAdd(-1) > 0) {
                    ctx.read();
                }
                RawClient.sunkBytes.getAndAdd(buf.readableBytes());
                client.sinkRef.get().next(buf);
            }
        }
        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            RawClient.channelInactiveCount.getAndAdd(1);
            LOGGER.debug("channelInactive  {}  {}", client.node.host, client.channelName);
            if (client.sinkTerm.compareAndExchange(0, 1) == 0) {
                client.sinkRef.get().complete();
            }
        }
        @Override
        public void channelUnregistered(ChannelHandlerContext ctx) {
            //LOGGER.info("channelUnregistered  {}  {}", client.node.host, client.channelName);
        }
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable e) {
            RawClient.channelExceptionCount.getAndAdd(1);
            LOGGER.error("in handler  {}  {}  {}", client.node.host, client.channelName, e.toString());
            e.printStackTrace();
            if (client.sinkTerm.compareAndExchange(0, 1) == 0) {
                client.sinkRef.get().error(e);
            }
            ctx.close();
        }
        RawClient client;
        ConcurrentLinkedDeque<ByteBuf> queue = new ConcurrentLinkedDeque<>();
    }

    static final NettyDataBufferFactory nfac = new NettyDataBufferFactory(UnpooledByteBufAllocator.DEFAULT);
    static final EventLoopGroup workerGroup = new NioEventLoopGroup(1);
    final AtomicInteger sinkTerm = new AtomicInteger();
    final AtomicInteger didCancel = new AtomicInteger();
    final AtomicInteger didDispose = new AtomicInteger();
    final AtomicLong requested = new AtomicLong();
    final AtomicReference<FluxSink<ByteBuf>> sinkRef = new AtomicReference<>();
    final AtomicReference<ChannelHandlerContext> ctxRef = new AtomicReference<>();
    static final AtomicLong channelActiveCount = new AtomicLong();
    static final AtomicLong channelInactiveCount = new AtomicLong();
    static final AtomicLong channelCloseCount = new AtomicLong();
    static final AtomicLong channelExceptionCount = new AtomicLong();
    static final AtomicLong channelReadCount = new AtomicLong();
    static final AtomicLong sinkRequestCount = new AtomicLong();
    static final AtomicLong sinkRequestItems = new AtomicLong();
    static final AtomicLong sunkBytes = new AtomicLong();
    Query subq;
    String channelName;
    Node node;

}