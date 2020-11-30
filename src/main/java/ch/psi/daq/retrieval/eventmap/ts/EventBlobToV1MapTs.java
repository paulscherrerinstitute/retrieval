package ch.psi.daq.retrieval.eventmap.ts;

import ch.psi.daq.retrieval.PositionedDatafile;
import ch.psi.daq.retrieval.ReqCtx;
import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DataBufferUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.util.function.Function;

public class EventBlobToV1MapTs implements Function<DataBuffer, Item> {
    static Logger LOGGER = (Logger) LoggerFactory.getLogger("EventBlobToV1MapTs");
    static final int HEADER_A_LEN = 2 * Integer.BYTES + 4 * Long.BYTES + 2 * Byte.BYTES;
    DataBufferFactory bufFac;
    DataBuffer left;
    int bufferSize;
    int bufferSize2;
    State state;
    int needMin;
    int missingBytes;
    long pulse;
    long ts;
    int blobLength;
    int headerLength;
    int optionalFieldsLength;
    long endNanos;
    Item item;
    ItemP itemP;
    int maxItemElements;
    String name;
    int termApplyCount;
    long itemCount;
    ReqCtx reqctx;

    enum State {
        EXPECT_HEADER_A,
        EXPECT_BLOBS,
        EXPECT_SECOND_LENGTH,
        TERM,
    }

    public enum Mock {
        NONE,
        DUMMY_ITEM,
        CHAIN_BUT_DUMMY,
    }

    public EventBlobToV1MapTs(ReqCtx reqctx, String name, long endNanos, DataBufferFactory bufferFactory, int bufferSize) {
        this.reqctx = reqctx;
        this.name = name;
        this.bufFac = bufferFactory;
        this.bufferSize = bufferSize;
        this.bufferSize2 = 2 * bufferSize;
        this.endNanos = endNanos;
        this.maxItemElements = 4;
        this.state = State.EXPECT_HEADER_A;
        this.needMin = HEADER_A_LEN;
    }

    public static Flux<Item> trans2(ReqCtx reqctx, Mock mock, Flux<DataBuffer> fl, String name, String channelName, long endNanos, DataBufferFactory bufFac, int bufferSize) {
        if (mock == Mock.DUMMY_ITEM) {
            return fl
            .doOnNext(DataBufferUtils::release)
            .map(buf -> Item.dummy(bufFac));
        }

        EventBlobToV1MapTs mapper;
        mapper = new EventBlobToV1MapTs(reqctx, name, endNanos, bufFac, bufferSize);
        return fl
        .doOnDiscard(Object.class, obj -> {
            if (obj == null) {
                LOGGER.error("{}  null obj in ed1ee40c", reqctx);
            }
            else if (obj instanceof DataBuffer) {
                LOGGER.info("{}  DataBuffer in ed1ee40c {}", reqctx, obj.getClass().getName());
                DataBufferUtils.release((DataBuffer) obj);
            }
            else if (obj instanceof PositionedDatafile) {
                LOGGER.info("{}  PositionedDatafile in ed1ee40c {}", reqctx, obj.getClass().getName());
                PositionedDatafile item = (PositionedDatafile) obj;
                item.release();
            }
            else {
                LOGGER.error("{}  Class in ed1ee40c {}", reqctx, obj.getClass().getName());
            }
        })
        .map(buf -> {
            if (mock == Mock.CHAIN_BUT_DUMMY) {
                DataBufferUtils.release(buf);
                return Item.dummy(bufFac);
            }
            else {
                try {
                    return mapper.apply(buf);
                }
                catch (Throwable e) {
                    LOGGER.error("{}  Mapper failure {}", reqctx, e.toString());
                    throw new RuntimeException(e);
                }
            }
        })
        .concatWith(Mono.defer(() -> {
            try {
                if (mock == Mock.CHAIN_BUT_DUMMY) {
                    Item item = Item.dummy(bufFac);
                    item.isLast = true;
                    return Mono.just(item);
                }
                else {
                    Item item = mapper.lastResult();
                    if (item == null) {
                        LOGGER.error("{}  lastResult is null  name {}", reqctx, name);
                        return Mono.just(Item.dummy(bufFac));
                    }
                    else {
                        return Mono.just(item);
                    }
                }
            }
            catch (Throwable e) {
                LOGGER.error("{}  lastResult error  name {}  {}", reqctx, name, e.toString());
                throw new RuntimeException(e);
            }
        }))
        .doOnTerminate(() -> mapper.release());
    }

    @Override
    public Item apply(DataBuffer buf) {
        if (buf.readableByteCount() == 0) {
            LOGGER.debug("{}  empty byte buffer received  {}", reqctx, name);
        }
        LOGGER.trace("{}  {}  apply  buf rp {}  buf wp {}  buf n {}", reqctx, name, buf.readPosition(), buf.writePosition(), buf.readableByteCount());
        if (state == State.TERM) {
            if (termApplyCount == 0) {
                LOGGER.trace("{}  {}  apply buffer despite TERM  c: {}", reqctx, name, termApplyCount);
            }
            else {
                LOGGER.warn("{}  {}  apply buffer despite TERM  c: {}", reqctx, name, termApplyCount);
            }
            termApplyCount += 1;
            DataBufferUtils.release(buf);
            item = new Item();
            itemCount += 1;
            item.term = true;
            return item;
        }
        else {
            Item ret = apply2(buf);
            LOGGER.trace("{}  {}  return Item  has item1 {}", reqctx, name, ret.item1 != null);
            return ret;
        }
    }

    public Item apply2(DataBuffer buf) {
        if (left == null) {
            item = new Item();
            itemCount += 1;
            item.item1 = new ItemP();
            itemP = item.item1;
            itemP.c = 0;
            itemP.ts = new long[maxItemElements];
            itemP.pos = new int[maxItemElements];
            itemP.ty = new int[maxItemElements];
            itemP.len = new int[maxItemElements];
            itemP.buf = buf;
            itemP.p1 = buf.readPosition();
        }
        else {
            item = new Item();
            itemCount += 1;
            if (needMin <= 0) {
                throw new RuntimeException("logic");
            }
            if (left.readableByteCount() <= 0) {
                throw new RuntimeException("logic");
            }
            if (left.readableByteCount() >= needMin) {
                throw new RuntimeException("logic");
            }
            int n = Math.min(buf.readableByteCount(), needMin - left.readableByteCount());
            int l1 = buf.readableByteCount();
            int l2 = left.readableByteCount();
            left.write(buf.slice(buf.readPosition(), n));
            buf.readPosition(buf.readPosition() + n);
            if (buf.readableByteCount() + n != l1) {
                throw new RuntimeException("logic");
            }
            if (left.readableByteCount() != l2 + n) {
                throw new RuntimeException("logic");
            }
            if (left.readableByteCount() >= needMin) {
                itemP = new ItemP();
                item.item1 = itemP;
                itemP.c = 0;
                itemP.ts = new long[maxItemElements];
                itemP.pos = new int[maxItemElements];
                itemP.ty = new int[maxItemElements];
                itemP.len = new int[maxItemElements];
                itemP.p1 = left.readPosition();
                parse(left);
                if (left.readableByteCount() != 0) {
                    throw new RuntimeException("logic");
                }
                itemP.p2 = left.readPosition();
                itemP.buf = left;
                left = null;
                itemP.buf.readPosition(itemP.p1);
                itemP.buf.writePosition(itemP.p2);
                itemP = new ItemP();
                item.item2 = itemP;
                itemP.c = 0;
                itemP.ts = new long[maxItemElements];
                itemP.pos = new int[maxItemElements];
                itemP.ty = new int[maxItemElements];
                itemP.len = new int[maxItemElements];
                itemP.p1 = buf.readPosition();
            }
            else {
                if (buf.readableByteCount() != 0) {
                    throw new RuntimeException("logic");
                }
                itemP = new ItemP();
                item.item1 = itemP;
                itemP.c = 0;
                itemP.ts = new long[maxItemElements];
                itemP.pos = new int[maxItemElements];
                itemP.ty = new int[maxItemElements];
                itemP.len = new int[maxItemElements];
                itemP.p1 = buf.readPosition();
                itemP.p2 = buf.readPosition();
            }
        }
        while (true) {
            if (state == State.TERM) {
                break;
            }
            if (buf.readableByteCount() == 0 || buf.readableByteCount() < needMin) {
                break;
            }
            parse(buf);
        }
        if (state != State.TERM && buf.readableByteCount() > 0) {
            if (needMin <= 0) {
                throw new RuntimeException("logic");
            }
            if (buf.readableByteCount() >= needMin) {
                throw new RuntimeException("logic");
            }
            itemP.p2 = buf.readPosition();
            left = bufFac.allocateBuffer(bufferSize2);
            left.write(buf.slice(buf.readPosition(), buf.readableByteCount()));
            buf.readPosition(buf.writePosition());
            itemP.buf = buf;
            itemP.buf.readPosition(itemP.p1);
            itemP.buf.writePosition(itemP.p2);
        }
        else {
            itemP.p2 = buf.readPosition();
            itemP.buf = buf;
            itemP.buf.readPosition(itemP.p1);
            itemP.buf.writePosition(itemP.p2);
        }
        Item ret = item;
        item = null;
        return ret;
    }

    void parse(DataBuffer buf) {
        if (buf == null) {
            throw new RuntimeException("logic");
        }
        if (state == State.EXPECT_HEADER_A) {
            parseHeaderA(buf);
        }
        else if (state == State.EXPECT_BLOBS) {
            parseBlob(buf);
        }
        else if (state == State.EXPECT_SECOND_LENGTH) {
            parseSecondLength(buf);
        }
        else if (state == State.TERM) {
        }
        else {
            throw new RuntimeException("logic");
        }
    }

    void parseHeaderA(DataBuffer buf) {
        int bpos = buf.readPosition();
        ByteBuffer bb = buf.asByteBuffer(buf.readPosition(), needMin);
        buf.readPosition(buf.readPosition() + needMin);
        int length = bb.getInt();
        if (length == 0) {
            LOGGER.warn("{}  {}  Stop because length == 0", reqctx, name);
            state = State.TERM;
            return;
        }
        if (length < 40 || length > 60 * 1024 * 1024) {
            LOGGER.error("{}  {}  Stop because unexpected  length {}", reqctx, name, length);
            state = State.TERM;
            return;
        }
        long ttl = bb.getLong();
        long ts = bb.getLong();
        long pulse = bb.getLong();
        long iocTime = bb.getLong();
        byte status = bb.get();
        byte severity = bb.get();
        LOGGER.trace("{}  {}  seen  length  {}  timestamp {} {}  pulse {}", reqctx, name, length, ts / 1000000000L, ts % 1000000000, pulse);
        if (ts >= endNanos) {
            LOGGER.debug("{}  {}  ts >= endNanos  {} {}  >=  {} {}", reqctx, name, ts / 1000000000L, ts % 1000000000, endNanos / 1000000000L, endNanos % 1000000000);
            state = State.TERM;
            buf.readPosition(bpos);
            buf.writePosition(bpos);
            return;
        }
        int optionalFieldsLength = bb.getInt();
        if (optionalFieldsLength < -1 || optionalFieldsLength == 0) {
            LOGGER.error("{}  {}  unexpected value for optionalFieldsLength: {}", reqctx, name, optionalFieldsLength);
            throw new RuntimeException("unexpected optional fields");
        }
        if (optionalFieldsLength != -1) {
            LOGGER.warn("{}  {}  Found optional fields: {}", reqctx, name, optionalFieldsLength);
        }
        if (optionalFieldsLength > 2048) {
            LOGGER.error("{}  {}  unexpected optional fields: {}", reqctx, name, optionalFieldsLength);
            throw new RuntimeException("unexpected optional fields");
        }
        headerLength = needMin;
        this.pulse = pulse;
        this.ts = ts;
        this.optionalFieldsLength = Math.max(optionalFieldsLength, 0);
        this.blobLength = length;
        reallocItem();
        itemP.ts[itemP.c] = ts;
        itemP.pos[itemP.c] = bpos;
        itemP.ty[itemP.c] = 1;
        itemP.len[itemP.c] = this.blobLength;
        itemP.c += 1;
        state = State.EXPECT_BLOBS;
        missingBytes = length - needMin - 4;
        needMin = 0;
    }

    void parseBlob(DataBuffer buf) {
        final int n = Math.min(buf.readableByteCount(), missingBytes);
        buf.readPosition(buf.readPosition() + n);
        missingBytes -= n;
        if (missingBytes > 0) {
            needMin = 0;
        }
        else {
            state = State.EXPECT_SECOND_LENGTH;
            needMin = Integer.BYTES;
        }
    }

    void parseSecondLength(DataBuffer buf) {
        int bpos = buf.readPosition();
        ByteBuffer bb = buf.asByteBuffer(buf.readPosition(), needMin);
        buf.readPosition(buf.readPosition() + needMin);
        int len2 = bb.getInt();
        if (len2 == -1) {
            LOGGER.warn("{}  {}  2nd length -1 encountered, ignoring", reqctx, name);
        }
        else if (len2 == 0) {
            LOGGER.warn("{}  {}  2nd length 0 encountered, ignoring", reqctx, name);
        }
        else if (len2 != blobLength) {
            LOGGER.error("{}  {}  event blob length mismatch at {}   {} vs {}", reqctx, name, buf.readPosition(), len2, blobLength);
            throw new RuntimeException("unexpected 2nd length");
        }
        reallocItem();
        itemP.ts[itemP.c] = ts;
        itemP.pos[itemP.c] = bpos + 4;
        itemP.ty[itemP.c] = 2;
        itemP.len[itemP.c] = -1;
        itemP.c += 1;
        state = State.EXPECT_HEADER_A;
        needMin = HEADER_A_LEN;
    }

    void reallocItem() {
        if (itemP.c >= maxItemElements) {
            if (maxItemElements >= 128 * 1024) {
                throw new RuntimeException("too many elements");
            }
            int n1 = maxItemElements;
            maxItemElements *= 4;
            long[] ts2 = itemP.ts;
            int[] pos2 = itemP.pos;
            int[] ty2 = itemP.ty;
            int[] len2 = itemP.len;
            itemP.ts = new long[maxItemElements];
            itemP.pos = new int[maxItemElements];
            itemP.ty = new int[maxItemElements];
            itemP.len = new int[maxItemElements];
            System.arraycopy(ts2, 0, itemP.ts, 0, n1);
            System.arraycopy(pos2, 0, itemP.pos, 0, n1);
            System.arraycopy(ty2, 0, itemP.ty, 0, n1);
            System.arraycopy(len2, 0, itemP.len, 0, n1);
        }
    }

    public void release() {
        LOGGER.debug("{}  {}  EventBlobToV1MapTs release", reqctx, name);
        if (left != null) {
            DataBufferUtils.release(left);
            left = null;
        }
    }

    public Item lastResult() {
        LOGGER.debug("{}  lastResult  name {}", reqctx, name);
        DataBuffer buf = bufFac.allocateBuffer(bufferSize);
        item = apply(buf);
        item.isLast = true;
        return item;
    }

}
