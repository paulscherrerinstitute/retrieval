package ch.psi.daq.retrieval.subnodes;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.EventLoop;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.ReadTimeoutException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentLinkedDeque;

class ChanHandler extends ChannelInboundHandlerAdapter {

    ChanHandler(RawClient client) {
        this.client = client;
    }

    @Override
    public void channelActive(final ChannelHandlerContext ctx) {
        RawClient.channelActiveCount.getAndAdd(1);
        long nreq;
        synchronized (client.ctxRef) {
            client.ctxRef.set(ctx);
            nreq = client.requested.get();
            client.readGuide += 1;
            if (nreq > 0) {
                RawClient.handlerFirstRead.getAndAdd(1);
                client.firstReadDecided.set(1);
            }
            else {
                RawClient.handlerNoFirstRead.getAndAdd(1);
            }
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
        EventLoop el = ctx.channel().eventLoop();
        if (el.inEventLoop()) {
            RawClient.handlerActiveLoopTrue.getAndAdd(1);
            ChannelFuture f = ctx.write(bbjs);
            ctx.flush();
            if (nreq > 0) {
                ctx.read();
            }
        }
        else {
            RawClient.handlerActiveLoopFalse.getAndAdd(1);
            el.execute(() -> {
                ctx.write(bbjs);
                ctx.flush();
                if (nreq > 0) {
                    ctx.read();
                }
            });
        }
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
        if (!(obj instanceof ByteBuf)) {
            throw new RuntimeException("incoming data is not buffer");
        }
        ByteBuf buf = (ByteBuf) obj;
        if (!gotHeaderLen) {
            header.limit(4);
            buf.readBytes(header);
            if (header.remaining() == 0) {
                header.flip();
                gotHeaderLen = true;
                int r = header.getInt(0);
                header.clear();
                header.limit(r);
            }
        }
        if (!gotHeaderBody) {
            buf.readBytes(header);
            if (header.remaining() == 0) {
                header.flip();
                gotHeaderBody = true;
                client.subStream.reqId = StandardCharsets.UTF_8.decode(header).toString();
            }
        }
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
        if (client.didCancel.get() != 0) {
            buf.release();
            ctx.close();
        }
        else if (client.didDispose.get() != 0) {
            buf.release();
            ctx.close();
        }
        else {
            long r = client.requested.getAndAdd(-1);
            if (r <= 0) {
                RawClient.LOGGER.error("channelRead  no demand  requested {}  node {}  channel {}", r, client.node, client.channelName);
                buf.release();
                ctx.close();
            }
            else {
                RawClient.sunkBytes.getAndAdd(buf.readableBytes());
                client.tsSunkenLast.set(System.nanoTime());
                client.sinkRef.get().next(buf);
                if (r > 1) {
                    EventLoop el = ctx.channel().eventLoop();
                    if (el.inEventLoop()) {
                        ctx.read();
                    }
                    else {
                        RawClient.LOGGER.error("channelRead  not in loop");
                        if (false) {
                            el.execute(ctx::read);
                        }
                    }
                }
            }
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object obj) {
        if (obj instanceof IdleStateEvent) {
            IdleStateEvent ev = (IdleStateEvent) obj;
            if (ev.state() == IdleState.READER_IDLE) {
                RawClient.handlerIdleRead.getAndAdd(1);
                //ctx.close();
            }
            if (ev.state() == IdleState.WRITER_IDLE) {
                RawClient.handlerIdleWrite.getAndAdd(1);
            }
            if (ev.state() == IdleState.ALL_IDLE) {
                RawClient.handlerIdleAll.getAndAdd(1);
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable e) {
        if (e instanceof ReadTimeoutException) {
            RawClient.LOGGER.error("ChanHandler read timeout  {}  {}", client.node, client.channelName);
            //ctx.close();
        }
        else {
            RawClient.channelExceptionCount.getAndAdd(1);
            RawClient.LOGGER.error("ChanHandler exceptionCaught  {}  {}  {}", client.node, client.channelName, e.toString());
            e.printStackTrace();
            if (client.sinkTerm.compareAndExchange(0, 1) == 0) {
                client.sinkRef.get().error(e);
            }
            ctx.close();
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        RawClient.channelInactiveCount.getAndAdd(1);
        RawClient.LOGGER.debug("channelInactive  {}  {}", client.node.host, client.channelName);
        if (client.sinkTerm.compareAndExchange(0, 1) == 0) {
            client.sinkRef.get().complete();
        }
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) {
        RawClient.LOGGER.debug("channelUnregistered  {}  {}", client.node.host, client.channelName);
    }

    RawClient client;
    ConcurrentLinkedDeque<ByteBuf> queue = new ConcurrentLinkedDeque<>();
    boolean gotHeaderLen;
    boolean gotHeaderBody;
    ByteBuffer header = ByteBuffer.allocate(100);

}
