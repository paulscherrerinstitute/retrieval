package ch.psi.daq.retrieval.subnodes;

import ch.psi.daq.retrieval.reqctx.ReqCtx;
import ch.psi.daq.retrieval.bytes.BufCont;
import ch.psi.daq.retrieval.config.Node;
import ch.psi.daq.retrieval.controller.QueryData;
import ch.psi.daq.retrieval.controller.raw.RawLocal;
import ch.psi.daq.retrieval.pod.api1.Query;
import ch.qos.logback.classic.Logger;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.core.io.buffer.DataBufferWrapper;
import org.springframework.core.io.buffer.DefaultDataBuffer;
import org.springframework.core.io.buffer.NettyDataBuffer;
import org.springframework.core.io.buffer.NettyDataBufferFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.netty.Connection;
import reactor.netty.NettyInbound;
import reactor.netty.NettyOutbound;
import reactor.netty.tcp.TcpClient;
import reactor.netty.tcp.TcpServer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class RawSub {
    static final Logger LOGGER = (Logger) LoggerFactory.getLogger(RawSub.class.getSimpleName());

    static Mono<SubStream> rawSub1(Query subq, String channelName, Node node) {
        AtomicInteger didCancel = new AtomicInteger();
        AtomicInteger didDispose = new AtomicInteger();
        AtomicLong requested = new AtomicLong();
        AtomicReference<FluxSink<ByteBuf>> sink = new AtomicReference<>();
        Flux<ByteBuf> fl3 = Flux.<ByteBuf>create(sink2 -> {
            sink2.onCancel(() -> {
                LOGGER.info("tcp raw client cancel from downstream");
                didCancel.set(1);
            });
            sink2.onDispose(() -> {
                LOGGER.info("tcp raw client dispose from downstream");
                didDispose.set(1);
            });
            sink2.onRequest(n -> {
                LOGGER.info("request from downstream {}", n);
                requested.getAndAdd(n);
            });
            sink.set(sink2);
        })
        .publishOn(pa3);
        NettyDataBufferFactory nfac = new NettyDataBufferFactory(UnpooledByteBufAllocator.DEFAULT);
        Mono<? extends Connection> conn = TcpClient.create()
        .host(node.host).port(node.port + 100)
        .doOnConnected(k -> {
            clientConnectedCount.getAndAdd(1);
            LOGGER.info("client connected to raw tcp  {}  {}", node.host, channelName);
        })
        .doOnDisconnected(k -> {
            clientDisconnectedCount.getAndAdd(1);
            LOGGER.info("client disconnected to raw tcp  {}  {}", node.host, channelName);
        })
        .handle((netin, netout) -> {
            String js5;
            try {
                js5 = new ObjectMapper().writeValueAsString(subq);
            }
            catch (IOException e) {
                js5 = "error";
            }
            ByteBuffer bq = ByteBuffer.allocate(1024);
            ByteBuffer jsbuf = StandardCharsets.UTF_8.encode(js5);
            //jsbuf.flip();
            LOGGER.info("putting js request size {}", jsbuf.remaining());
            bq.putInt(jsbuf.remaining());
            bq.put(jsbuf);
            bq.flip();
            ByteBuf bbjs = Unpooled.wrappedBuffer(bq);
            LOGGER.info("raw client sending request  to {}  size {}  {}", node.host, bbjs.readableBytes(), js5);
            return netout
            .send(Mono.just(bbjs)).then()
            .then(Mono.defer(() -> {
                LOGGER.info("make deferred mono");
                return netin
                .receive()
                .takeWhile(k -> {
                    if (didCancel.get() == 0) {
                        return true;
                    }
                    else {
                        k.release();
                        return false;
                    }
                })
                .flatMap(k -> {
                    final long del = 1000L * 1000 * 10;
                    int ii2 = 0;
                    while (true) {
                        if (didCancel.get() != 0) {
                            LOGGER.info("upstream abort due to cancel");
                            return Mono.error(new RuntimeException("didCancel"));
                        }
                        else {
                            if (requested.get() > 0) {
                                k.retain();
                                requested.getAndAdd(-1);
                                clientSunkBytes.getAndAdd(k.readableBytes());
                                sink.get().next(k);
                                break;
                            }
                            else {
                                long tss1 = System.nanoTime();
                                while (System.nanoTime() - tss1 < del) {
                                    try {
                                        Thread.sleep(1);
                                    }
                                    catch (InterruptedException e) {
                                    }
                                    if (requested.get() > 0) {
                                        sinkBreakNewRequest.getAndAdd(1);
                                        break;
                                    }
                                    if (didCancel.get() != 0) {
                                        sinkBreakDidCancel.getAndAdd(1);
                                        LOGGER.warn("sink break didCancel  {}  {}", node, channelName);
                                        break;
                                    }
                                }
                            }
                        }
                        ii2 += 1;
                        if (ii2 % 500 == 0) {
                            LOGGER.warn("no demand  req {}  cancel {}  node {}  {}", requested.get(), didCancel.get(), node, channelName);
                        }
                        if (ii2 > 1000L * 1000 * 1000 * 120 / del) {
                            LOGGER.warn("no demand  req {}  cancel {}  node {}  {}", requested.get(), didCancel.get(), node, channelName);
                            LOGGER.error("raw tcp client giving up  node {}  {}", node, channelName);
                            return Mono.error(new RuntimeException("tcp raw client giving up timeout"));
                        }
                    }
                    return Mono.empty();
                })
                .doFinally(k -> {
                    sink.get().complete();
                })
                .then();
            }));
        })
        .connect();
        return conn
        .map(conn2 -> conn2)
        //.subscribeOn(Schedulers.newParallel(channelName + "-" + node.host, 15))
        .subscribeOn(pa2)
        .publishOn(pa2)
        //.subscribe();
        //return Mono.just(0)
        .map(conn2 -> {
            SubStream subs = new SubStream();
            subs.reqId = String.format("raw-tcp-%s-%s", node.host, channelName);
            subs.fl = fl3
            .map(k -> BufCont.fromBuffer(nfac.wrap(k), BufCont.Mark.RawSub1));
            return subs;
        })
        .doOnSubscribe(k -> {
            LOGGER.info("subscribed to tcp future  thread {}", Thread.currentThread().getName());
        });
    }

    static Mono<SubStream> rawSub2(Query subq, String channelName, Node node) {
        AtomicInteger didCancel = new AtomicInteger();
        AtomicInteger didDispose = new AtomicInteger();
        AtomicLong requested = new AtomicLong();
        AtomicReference<FluxSink<ByteBuf>> sink = new AtomicReference<>();
        Flux<ByteBuf> fl3 = Flux.<ByteBuf>create(sink2 -> {
            sink2.onCancel(() -> {
                LOGGER.info("tcp raw client cancel from downstream");
                didCancel.set(1);
            });
            sink2.onDispose(() -> {
                LOGGER.info("tcp raw client dispose from downstream");
                didDispose.set(1);
            });
            sink2.onRequest(n -> {
                LOGGER.info("request from downstream {}", n);
                requested.getAndAdd(n);
            });
            sink.set(sink2);
        });

        AtomicReference<Flux<ByteBuf>> fl5 = new AtomicReference<>();

        Mono<? extends Connection> conn = TcpClient.create()
        .host(node.host).port(node.port + 100)
        .doOnConnected(k -> {
            clientConnectedCount.getAndAdd(1);
            LOGGER.info("client connected to raw tcp  {}  {}", node.host, channelName);
        })
        .doOnDisconnected(k -> {
            clientDisconnectedCount.getAndAdd(1);
            LOGGER.info("client disconnected to raw tcp  {}  {}", node.host, channelName);
        })
        .handle((netin, netout) -> {
            String js5;
            try {
                js5 = new ObjectMapper().writeValueAsString(subq);
            }
            catch (IOException e) {
                js5 = "error";
            }
            ByteBuffer bq = ByteBuffer.allocate(1024);
            ByteBuffer jsbuf = StandardCharsets.UTF_8.encode(js5);
            //jsbuf.flip();
            LOGGER.info("putting js request size {}", jsbuf.remaining());
            bq.putInt(jsbuf.remaining());
            bq.put(jsbuf);
            bq.flip();
            ByteBuf bbjs = Unpooled.wrappedBuffer(bq);
            LOGGER.info("raw client sending request  to {}  size {}  {}", node.host, bbjs.readableBytes(), js5);
            Flux<ByteBuf> fl4 = netout
            .send(Mono.just(bbjs)).then()
            .flatMapMany(k -> netin.receive());
            fl5.set(fl4);

            // I can't break out here!

            return null;

        })
        .connect();
        return conn
        .map(conn2 -> conn2)
        //.subscribeOn(Schedulers.newParallel(channelName + "-" + node.host, 15))
        .subscribeOn(Schedulers.boundedElastic())
        .publishOn(sc2)
        //.subscribe();
        //return Mono.just(0)
        .map(conn2 -> {
            SubStream subs = new SubStream();
            subs.reqId = String.format("raw-tcp-%s-%s", node.host, channelName);
            subs.fl = fl3
            .map(k -> BufCont.fromBuffer(nfac.wrap(k), BufCont.Mark.RawSub2));
            return subs;
        })
        .doOnSubscribe(k -> {
            LOGGER.info("subscribed to tcp future  thread {}", Thread.currentThread().getName());
        });
    }


    public void rawTcp() {
        LOGGER.info("service start");
        String host = "0.0.0.0";
        TcpServer.create()
        .host(host).port(queryData.port + 100)
        .doOnConnection(k -> {
            k.onTerminate()
            .doFinally(q -> {
                if (q == SignalType.ON_COMPLETE) {
                    hostTerminatedCount.getAndAdd(1);
                }
                else {
                    hostTerminatedErrCount.getAndAdd(1);
                }
            }).subscribe();
            k.onDispose()
            .doFinally(q -> {
                if (q == SignalType.ON_COMPLETE) {
                    hostDisposedCount.getAndAdd(1);
                }
                else {
                    hostDisposedErrCount.getAndAdd(1);
                }
            }).subscribe();
            hostConnectedCount.getAndAdd(1);
            LOGGER.debug("RawSub  got connection  {}", k.address().toString());
        })
        .handle(this::handle2)
        .bind()
        .subscribeOn(be1)
        .block();

        /*
        TcpClient.create()
        .wiretap(true)
        .host(host).port(port)
        .doOnConnected(k -> {
            LOGGER.info("connceted");
        })
        .handle(this::req1)
        .connect()
        .subscribeOn(Schedulers.boundedElastic())
        .block()
        .onDispose()
        .doOnTerminate(() -> LOGGER.info("client terminated"))
        .block();
        */

        //Mono.empty().delaySubscription(Duration.ofMillis(3000)).block();
    }

    Mono<Void> handle1(NettyInbound netin, NettyOutbound netout) {
        ByteBuf allbuf = Unpooled.buffer(1024 * 8);
        return netin
        .withConnection(conn -> {})
        .receive()
        .concatMap(bb -> {
            LOGGER.info("handle1 got buffer {}", bb.readableBytes());
            allbuf.writeBytes(bb);
            if (allbuf.readableBytes() >= 4) {
                if (false) {
                    int h = Math.min(32, allbuf.readableBytes()) + allbuf.readerIndex();
                    for (int i2 = allbuf.readerIndex(); i2 < h; i2 += 1) {
                        LOGGER.info("{}  {}", i2, allbuf.getByte(i2));
                    }
                }
                int len1 = allbuf.getInt(0);
                if (allbuf.readableBytes() >= len1) {
                    allbuf.readInt();
                    byte[] a = new byte[len1];
                    int p1 = allbuf.readerIndex();
                    allbuf.readBytes(a);
                    int p2 = allbuf.readerIndex();
                    if (p1 == p2) {
                        LOGGER.error("handle1 unchanged pos");
                        return Mono.error(new RuntimeException("read into array changed position"));
                    }
                    try {
                        Query query = new ObjectMapper().readValue(a, Query.class);
                        LOGGER.info("handle1 query {}", new ObjectMapper().writeValueAsString(query));
                        ReqCtx reqCtx = ReqCtx.dummy();
                        Mono<Flux<DataBuffer>> flm = new RawLocal().rawLocalInner(reqCtx, queryData, Mono.just(query));
                        return flm
                        .doOnError(e -> {
                            LOGGER.error("raw pipe got error 1  {}", e.toString());
                        })
                        .flatMap(fl -> {
                            return fl
                            .doOnError(e -> {
                                LOGGER.error("raw pipe got error 2  {}", e.toString());
                            })
                            .doOnTerminate(() -> {
                                LOGGER.info("doOnTerminate rawLocalInner flux");
                            })
                            .doFinally(k -> {
                                LOGGER.info("doFinally rawLocalInner flux");
                            })
                            .concatMap(buf -> {
                                //LOGGER.info("got buffer to send  {}", buf.readableByteCount());
                                if (buf instanceof NettyDataBuffer) {
                                    NettyDataBuffer ndb = (NettyDataBuffer) buf;
                                    ByteBuf bb3 = ndb.getNativeBuffer();
                                    //LOGGER.info("send from NettyDataBuffer  {}", bb3.readableBytes());
                                    hostEmittedBytes.getAndAdd(bb3.readableBytes());
                                    return netout.send(Mono.just(bb3)).then();
                                }
                                else if (buf instanceof DefaultDataBuffer) {
                                    ByteBuffer bb2 = buf.asByteBuffer(buf.readPosition(), buf.readableByteCount());
                                    ByteBuf bb3 = Unpooled.buffer(buf.readableByteCount());
                                    bb3.writeBytes(bb2);
                                    //buf.read(bb3.array(), bb3.arrayOffset(), bb3.readableBytes());
                                    DataBufferUtils.release(buf);
                                    //LOGGER.info("send from DefaultDataBuffer  {}", bb3.readableBytes());
                                    hostEmittedBytes.getAndAdd(bb3.readableBytes());
                                    return netout.send(Mono.just(bb3)).then();
                                }
                                else if (buf instanceof DataBufferWrapper) {
                                    ByteBuffer bb2 = buf.asByteBuffer(buf.readPosition(), buf.readableByteCount());
                                    ByteBuf bb3 = Unpooled.buffer(buf.readableByteCount());
                                    bb3.writeBytes(bb2);
                                    //buf.read(bb3.array(), bb3.arrayOffset(), bb3.readableBytes());
                                    DataBufferUtils.release(buf);
                                    //LOGGER.info("send from DataBufferWrapper  {}", bb3.readableBytes());
                                    hostEmittedBytes.getAndAdd(bb3.readableBytes());
                                    return netout.send(Mono.just(bb3)).then();
                                }
                                else if (buf != null) {
                                    LOGGER.error("unhandled buffer kind  {}", buf.getClass().getSimpleName());
                                    return Mono.error(new RuntimeException("unknown buffer kind"));
                                }
                                else {
                                    return Mono.error(new RuntimeException("null buffer"));
                                }
                            }, 0)
                            .then();
                        });
                    }
                    catch (IOException e) {
                        LOGGER.error("error IOException {}", e.toString());
                        return Mono.error(e);
                    }
                }
                else {
                    LOGGER.info("no full command yet");
                    return Mono.empty();
                }
            }
            else {
                LOGGER.info("not minimum length yet");
                return Mono.empty();
            }
        }, 0)
        .then();
    }

    Mono<Void> handle2(NettyInbound netin, NettyOutbound netout) {
        ByteBuf allbuf = Unpooled.buffer(1024 * 8);
        return netin
        .withConnection(conn -> {})
        .receive()
        .concatMap(bb -> {
            LOGGER.debug("RawSub  handle host {}  got {} bytes", queryData.conf.canonicalHostname, bb.readableBytes());
            allbuf.writeBytes(bb);
            if (allbuf.readableBytes() >= 4) {
                if (false) {
                    int h = Math.min(32, allbuf.readableBytes()) + allbuf.readerIndex();
                    for (int i2 = allbuf.readerIndex(); i2 < h; i2 += 1) {
                        LOGGER.debug("{}  {}", i2, allbuf.getByte(i2));
                    }
                }
                int len1 = allbuf.getInt(0);
                if (allbuf.readableBytes() >= len1) {
                    allbuf.readInt();
                    byte[] a = new byte[len1];
                    allbuf.readBytes(a);
                    try {
                        Query query = new ObjectMapper().readValue(a, Query.class);
                        LOGGER.debug("handle2  query {}", new ObjectMapper().writeValueAsString(query));
                        return Mono.just(query);
                    }
                    catch (IOException e) {
                        LOGGER.error("error IOException {}", e.toString());
                        return Mono.error(e);
                    }
                }
                else {
                    return Mono.empty();
                }
            }
            else {
                return Mono.empty();
            }
        }, 0)
        .take(1)
        .concatMap(query -> {
            ReqCtx reqCtx = ReqCtx.fromRawRequest(queryData.requestStatusBoard());
            Mono<Flux<DataBuffer>> flm = new RawLocal().rawLocalInner(reqCtx, queryData, Mono.just(query));
            return flm
            .doOnCancel(() -> LOGGER.warn("rawLocalInner doOnCancel"))
            .doFinally(k -> LOGGER.debug("rawLocalInner doFinally  {}", k))
            .doOnError(e -> {
                errorCount.getAndAdd(1);
                LOGGER.error("{}  raw pipe got error 1  {}", reqCtx, e.toString());
            })
            .flatMap(fl -> {
                DataBuffer b1 = reqCtx.bufCtx.bufFac.allocateBuffer(reqCtx.bufCtx.bufferSize);
                ByteBuffer b2 = ByteBuffer.allocate(10);
                b2.putInt(reqCtx.reqId.length());
                b2.flip();
                b1.write(b2);
                b1.write(reqCtx.reqId.getBytes(StandardCharsets.UTF_8));
                return fl
                .startWith(b1)
                .doOnError(e -> {
                    errorCount.getAndAdd(1);
                    LOGGER.error("{}  raw pipe got error 2  {}", reqCtx, e.toString());
                })
                .concatMap(buf -> {
                    //LOGGER.info("got buffer to send  {}", buf.readableByteCount());
                    //LOGGER.info("{}  rawLocalInner emit:\n{}", reqCtx, BufCont.dumpContent(buf));
                    if (buf instanceof NettyDataBuffer) {
                        NettyDataBuffer ndb = (NettyDataBuffer) buf;
                        ByteBuf bb3 = ndb.getNativeBuffer();
                        //LOGGER.info("send from NettyDataBuffer  {}", bb3.readableBytes());
                        hostEmittedBytes.getAndAdd(bb3.readableBytes());
                        return netout.send(Mono.just(bb3))
                        .then()
                        .doOnError(e -> {
                            LOGGER.warn("send error A {}", e.toString());
                            DataBufferUtils.release(buf);
                        })
                        .doOnCancel(() -> LOGGER.warn("send cancel"))
                        ;
                    }
                    else if (buf instanceof DefaultDataBuffer) {
                        ByteBuffer bb2 = buf.asByteBuffer(buf.readPosition(), buf.readableByteCount());
                        ByteBuf bb3 = Unpooled.buffer(buf.readableByteCount());
                        bb3.writeBytes(bb2);
                        //buf.read(bb3.array(), bb3.arrayOffset(), bb3.readableBytes());
                        DataBufferUtils.release(buf);
                        //LOGGER.info("send from DefaultDataBuffer  {}", bb3.readableBytes());
                        hostEmittedBytes.getAndAdd(bb3.readableBytes());
                        return netout.send(Mono.just(bb3))
                        .then()
                        .doOnError(e -> {
                            LOGGER.warn("send error B {}", e.toString());
                            DataBufferUtils.release(buf);
                        })
                        .doOnCancel(() -> LOGGER.warn("send cancel"))
                        ;
                    }
                    else if (buf instanceof DataBufferWrapper) {
                        ByteBuffer bb2 = buf.asByteBuffer(buf.readPosition(), buf.readableByteCount());
                        ByteBuf bb3 = Unpooled.buffer(buf.readableByteCount());
                        bb3.writeBytes(bb2);
                        //buf.read(bb3.array(), bb3.arrayOffset(), bb3.readableBytes());
                        DataBufferUtils.release(buf);
                        //LOGGER.info("send from DataBufferWrapper  {}", bb3.readableBytes());
                        hostEmittedBytes.getAndAdd(bb3.readableBytes());
                        return netout.send(Mono.just(bb3))
                        .then()
                        .doOnError(e -> {
                            LOGGER.warn("send error C {}", e.toString());
                            DataBufferUtils.release(buf);
                        })
                        .doOnCancel(() -> LOGGER.warn("send cancel"))
                        ;
                    }
                    else if (buf != null) {
                        errorCount.getAndAdd(1);
                        LOGGER.error("{}  unhandled buffer kind  {}", reqCtx, buf.getClass().getSimpleName());
                        DataBufferUtils.release(buf);
                        return Mono.error(new RuntimeException("unknown buffer kind"));
                    }
                    else {
                        errorCount.getAndAdd(1);
                        LOGGER.error("{}  send null buffer", reqCtx);
                        return Mono.error(new RuntimeException("null buffer"));
                    }
                }, 0)
                .then();
            })
            .doFinally(k -> {});
        }, 0)
        .doFinally(k -> {
            hostFinallyCount.getAndAdd(1);
        })
        .then();
    }

    public RawSub(QueryData queryData) {
        this.queryData = queryData;
    }

    public static class Stats {
        public long hostConnectedCount;
        public long hostDisconnectedCount;
        public long hostTerminatedCount;
        public long hostTerminatedErrCount;
        public long hostDisposedCount;
        public long hostDisposedErrCount;
        public long hostFinallyCount;
        public long hostEmittedBytes;
        public long clientConnectedCount;
        public long clientDisconnectedCount;
        public long clientSunkBytes;
        public long sinkBreakNewRequest;
        public long sinkBreakDidCancel;
        public long errorCount;
        public Stats() {
            hostConnectedCount = RawSub.hostConnectedCount.get();
            hostDisconnectedCount = RawSub.hostDisconnectedCount.get();
            hostTerminatedCount = RawSub.hostTerminatedCount.get();
            hostTerminatedErrCount = RawSub.hostTerminatedErrCount.get();
            hostDisposedCount = RawSub.hostDisposedCount.get();
            hostDisposedErrCount = RawSub.hostDisposedErrCount.get();
            hostFinallyCount = RawSub.hostFinallyCount.get();
            hostEmittedBytes = RawSub.hostEmittedBytes.get();
            clientConnectedCount = RawSub.clientConnectedCount.get();
            clientDisconnectedCount = RawSub.clientDisconnectedCount.get();
            clientSunkBytes = RawSub.clientSunkBytes.get();
            errorCount = RawSub.errorCount.get();
            sinkBreakNewRequest = RawSub.sinkBreakNewRequest.get();
            sinkBreakDidCancel = RawSub.sinkBreakDidCancel.get();
        }
    }

    static final AtomicLong hostConnectedCount = new AtomicLong();
    static final AtomicLong hostDisconnectedCount = new AtomicLong();
    static final AtomicLong hostTerminatedCount = new AtomicLong();
    static final AtomicLong hostTerminatedErrCount = new AtomicLong();
    static final AtomicLong hostDisposedCount = new AtomicLong();
    static final AtomicLong hostDisposedErrCount = new AtomicLong();
    static final AtomicLong hostFinallyCount = new AtomicLong();
    static final AtomicLong hostEmittedBytes = new AtomicLong();
    static final AtomicLong clientConnectedCount = new AtomicLong();
    static final AtomicLong clientDisconnectedCount = new AtomicLong();
    static final AtomicLong clientSunkBytes = new AtomicLong();
    static final AtomicLong sinkBreakNewRequest = new AtomicLong();
    static final AtomicLong sinkBreakDidCancel = new AtomicLong();
    static final AtomicLong errorCount = new AtomicLong();
    static final NettyDataBufferFactory nfac = new NettyDataBufferFactory(UnpooledByteBufAllocator.DEFAULT);
    static final Scheduler sc2 = null;
    static final Scheduler pa2 = null;
    static final Scheduler pa3 = null;
    static final Scheduler be1 = Schedulers.newBoundedElastic(64, 512, "be1");

    QueryData queryData;

}
