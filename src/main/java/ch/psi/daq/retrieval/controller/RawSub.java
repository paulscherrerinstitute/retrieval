package ch.psi.daq.retrieval.controller;

import ch.psi.daq.retrieval.ReqCtx;
import ch.psi.daq.retrieval.config.Node;
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
    static final AtomicLong hostConnectedCount = new AtomicLong();
    static final AtomicLong hostDisconnectedCount = new AtomicLong();
    static final AtomicLong hostTerminatedCount = new AtomicLong();
    static final AtomicLong hostDisposedCount = new AtomicLong();
    static final AtomicLong hostFinallyCount = new AtomicLong();
    static final AtomicLong hostEmittedBytes = new AtomicLong();
    static final AtomicLong clientConnectedCount = new AtomicLong();
    static final AtomicLong clientDisconnectedCount = new AtomicLong();
    static final AtomicLong clientSunkBytes = new AtomicLong();
    static final AtomicLong errorCount = new AtomicLong();
    static final NettyDataBufferFactory nfac = new NettyDataBufferFactory(UnpooledByteBufAllocator.DEFAULT);

    public static class Stats {
        public long hostConnectedCount;
        public long hostDisconnectedCount;
        public long hostTerminatedCount;
        public long hostDisposedCount;
        public long hostFinallyCount;
        public long hostEmittedBytes;
        public long clientConnectedCount;
        public long clientDisconnectedCount;
        public long clientSunkBytes;
        public long errorCount;
        public Stats() {
            hostConnectedCount = RawSub.hostConnectedCount.get();
            hostDisconnectedCount = RawSub.hostDisconnectedCount.get();
            hostTerminatedCount = RawSub.hostTerminatedCount.get();
            hostDisposedCount = RawSub.hostDisposedCount.get();
            hostFinallyCount = RawSub.hostFinallyCount.get();
            hostEmittedBytes = RawSub.hostEmittedBytes.get();
            clientConnectedCount = RawSub.clientConnectedCount.get();
            clientDisconnectedCount = RawSub.clientDisconnectedCount.get();
            clientSunkBytes = RawSub.clientSunkBytes.get();
            errorCount = RawSub.errorCount.get();
        }
    }


    static Mono<SubStream> rawSub(Query subq, String channelName, Node node) {
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
                .takeWhile(k -> didCancel.get() == 0)
                //.doOnNext(k -> k.retain())
                //.delayElements(Duration.ofMillis(10))
                .flatMap(k -> {
                    //LOGGER.info("next incoming buffer to put in to sink");
                    int ii2 = 0;
                    while (true) {
                        if (didCancel.get() != 0) {
                            LOGGER.info("upstream abort due to cancel");
                            //k.release();
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
                                while (System.nanoTime() - tss1 < 1000L * 1000 * 10) {
                                    try {
                                        Thread.sleep(1);
                                    }
                                    catch (InterruptedException e) {
                                    }
                                    if (requested.get() > 0) {
                                        break;
                                    }
                                }
                            }
                        }
                        ii2 += 1;
                        if (ii2 % 100 == 0) {
                            LOGGER.info("no demand  {}  {}", requested.get(), node.host);
                        }
                        if (ii2 > 100 * 20) {
                            LOGGER.info("raw tcp client giving up");
                            //k.release();
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
            .map(k -> {
                return nfac.wrap(k);
            });
            return subs;
        })
        .doOnSubscribe(k -> {
            LOGGER.info("subscribed to tcp future  thread {}", Thread.currentThread().getName());
        });
    }


    static final Scheduler sc2 = Schedulers.newBoundedElastic(32, 32, "subreq");
    static final Scheduler pa2 = Schedulers.newParallel("pa2", 128);
    static final Scheduler pa3 = Schedulers.newParallel("pa3", 128);



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
            .map(k -> {
                return nfac.wrap(k);
            });
            return subs;
        })
        .doOnSubscribe(k -> {
            LOGGER.info("subscribed to tcp future  thread {}", Thread.currentThread().getName());
        });
    }


    public void rawTcp() {
        LOGGER.info("raw start");
        String host = "0.0.0.0";
        TcpServer.create()
        .host(host).port(queryData.port + 100)
        .doOnConnection(k -> {
            hostConnectedCount.getAndAdd(1);
            LOGGER.info("rawTcp  CONNECTION");
        })
        .handle(this::handle2)
        .bind()
        .subscribeOn(Schedulers.parallel())
        .block();

        LOGGER.info("raw started");

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
        .withConnection(conn -> {
            conn.onDispose(() -> hostDisposedCount.getAndAdd(1));
        })
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
                        LOGGER.info("handle1 query: {}  json {}", query, new ObjectMapper().writeValueAsString(query));
                        ReqCtx reqCtx = ReqCtx.dummy();
                        String mainReqId = "mainReqIdUNKNOWN";
                        Mono<Flux<DataBuffer>> flm = queryData.rawLocalInner(reqCtx, Mono.just(query), mainReqId);
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
        .withConnection(conn -> {
            conn.onDispose(() -> hostDisposedCount.getAndAdd(1));
        })
        .receive()
        .concatMap(bb -> {
            LOGGER.info("handle2 got buffer {}", bb.readableBytes());
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
                    allbuf.readBytes(a);
                    try {
                        Query query = new ObjectMapper().readValue(a, Query.class);
                        LOGGER.info("handle2 query: {}  json {}", query, new ObjectMapper().writeValueAsString(query));
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
            ReqCtx reqCtx = ReqCtx.dummy();
            String mainReqId = "mainReqIdUNKNOWN";
            Mono<Flux<DataBuffer>> flm = queryData.rawLocalInner(reqCtx, Mono.just(query), mainReqId);
            return flm
            .doOnError(e -> {
                errorCount.getAndAdd(1);
                LOGGER.error("raw pipe got error 1  {}", e.toString());
            })
            .doOnNext(k -> {
                LOGGER.info("got flm");
            })
            .flatMap(fl -> {
                return fl
                .doOnError(e -> {
                    errorCount.getAndAdd(1);
                    LOGGER.error("raw pipe got error 2  {}", e.toString());
                })
                .doFinally(k -> {
                    //LOGGER.info("doFinally rawLocalInner flux");
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
                        errorCount.getAndAdd(1);
                        LOGGER.error("unhandled buffer kind  {}", buf.getClass().getSimpleName());
                        return Mono.error(new RuntimeException("unknown buffer kind"));
                    }
                    else {
                        errorCount.getAndAdd(1);
                        return Mono.error(new RuntimeException("null buffer"));
                    }
                }, 0)
                .doFinally(k -> {
                    //LOGGER.info("doFinally rawLocalInner flux");
                })
                .then();
            });
        }, 0)
        .doFinally(k -> {
            hostFinallyCount.getAndAdd(1);
            LOGGER.info("sendpipe doFinally");
        })
        .then();
    }

    public RawSub(QueryData queryData) {
        this.queryData = queryData;
    }

    QueryData queryData;

}
