package ch.psi.daq.retrieval.eventmap.ts;

import ch.psi.daq.retrieval.BufCtx;
import ch.psi.daq.retrieval.ReqCtx;
import ch.psi.daq.retrieval.bytes.BufCont;
import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import reactor.core.publisher.Flux;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static ch.psi.daq.retrieval.controller.QueryData.doDiscard;

public class EventBlobToV1MapTs implements Function<BufCont, MapTsItemVec> {
    static final Logger LOGGER = (Logger) LoggerFactory.getLogger(EventBlobToV1MapTs.class.getSimpleName());
    static final int HEADER_A_LEN = 2 * Integer.BYTES + 4 * Long.BYTES + 2 * Byte.BYTES;
    static final AtomicLong leftCopiedTimes = new AtomicLong();
    static final AtomicLong leftCopiedBytes = new AtomicLong();
    static final AtomicLong applyBufContEmpty = new AtomicLong();
    static final AtomicLong applyDespiteTerm = new AtomicLong();
    static final AtomicLong applyEmptyBuffer = new AtomicLong();
    static final AtomicLong transInpDiscardCount = new AtomicLong();
    static final AtomicLong openCount = new AtomicLong();
    static final AtomicLong closeCount = new AtomicLong();
    static final AtomicLong retainLeftCount = new AtomicLong();
    static final AtomicLong inpCount = new AtomicLong();
    static final AtomicLong inpBytes = new AtomicLong();
    static final AtomicLong leftWithoutBuf = new AtomicLong();
    BufCont leftbufcont;
    State state;
    int needMin;
    int missingBytes;
    long pulse;
    long ts;
    int blobLength;
    int headerLength;
    int optionalFieldsLength;
    long endNanos;
    int markBeg;
    int inside;
    int bufBeg;
    String name;
    ReqCtx reqctx;
    boolean closed;
    boolean detectedUnordered;
    long tsLast = Long.MIN_VALUE;
    long pulseLast = Long.MIN_VALUE;

    enum State {
        EXPECT_HEADER_A,
        EXPECT_BLOBS,
        EXPECT_SECOND_LENGTH,
        TERM,
    }

    public EventBlobToV1MapTs(ReqCtx reqctx, String name, long endNanos, BufCtx bufctx) {
        openCount.getAndAdd(1);
        this.reqctx = reqctx;
        this.name = name;
        this.endNanos = endNanos;
        this.state = State.EXPECT_HEADER_A;
        this.needMin = HEADER_A_LEN;
    }

    public static class Stats {
        public long leftCopiedTimes;
        public long leftCopiedBytes;
        public long applyBufContEmpty;
        public long applyDespiteTerm;
        public long applyEmptyBuffer;
        public long transInpDiscardCount;
        public long openCount;
        public long closeCount;
        public long retainLeftCount;
        public long inpCount;
        public long inpBytes;
        public long leftWithoutBuf;
        public Stats() {
            leftCopiedTimes = EventBlobToV1MapTs.leftCopiedTimes.get();
            leftCopiedBytes = EventBlobToV1MapTs.leftCopiedBytes.get();
            applyBufContEmpty = EventBlobToV1MapTs.applyBufContEmpty.get();
            applyDespiteTerm = EventBlobToV1MapTs.applyDespiteTerm.get();
            applyEmptyBuffer = EventBlobToV1MapTs.applyEmptyBuffer.get();
            transInpDiscardCount = EventBlobToV1MapTs.transInpDiscardCount.get();
            openCount = EventBlobToV1MapTs.openCount.get();
            closeCount = EventBlobToV1MapTs.closeCount.get();
            retainLeftCount = EventBlobToV1MapTs.retainLeftCount.get();
            inpCount = EventBlobToV1MapTs.inpCount.get();
            inpBytes = EventBlobToV1MapTs.inpBytes.get();
            leftWithoutBuf = EventBlobToV1MapTs.leftWithoutBuf.get();
        }
    }

    public static Flux<MapTsItemVec> trans(ReqCtx reqctx, Flux<BufCont> fl, String name, String channelName, long endNanos, BufCtx bufctx) {
        final EventBlobToV1MapTs mapper = new EventBlobToV1MapTs(reqctx, name, endNanos, bufctx);
        return fl
        .doOnDiscard(BufCont.class, BufCont::close)
        .map(mapper)
        .doFinally(k -> mapper.release())
        .transform(doDiscard("EventBlobToV1MapTsTrans"))
        .doOnNext(obj -> obj.markWith(BufCont.Mark.MapTs_trans2));
    }

    @Override
    public synchronized MapTsItemVec apply(BufCont bufcont) {
        if (bufcont.isEmpty()) {
            applyBufContEmpty.getAndAdd(1);
            bufcont.close();
            return MapTsItemVec.empty();
        }
        final DataBuffer buf = bufcont.bufferRef();
        if (state == State.TERM) {
            applyDespiteTerm.getAndAdd(1);
            bufcont.close();
            return MapTsItemVec.term();
        }
        else {
            if (buf.readableByteCount() == 0) {
                applyEmptyBuffer.getAndAdd(1);
            }
            inpCount.getAndAdd(1);
            inpBytes.getAndAdd(buf.readableByteCount());
            MapTsItemVec ret = applyInner(bufcont);
            ret.markWith(BufCont.Mark.MapTs_apply);
            return ret;
        }
    }

    MapTsItemVec applyInner(BufCont bufcont) {
        MapTsItemVec ret = MapTsItemVec.empty();
        DataBuffer buf = bufcont.bufferRef();
        if (leftbufcont != null && !leftbufcont.hasBuf()) {
            leftWithoutBuf.getAndAdd(1);
            leftbufcont.close();
            leftbufcont = null;
        }
        if (leftbufcont != null && leftbufcont.hasBuf()) {
            applyLeft(buf, ret);
        }
        markBeg = -1;
        bufBeg = buf.readPosition();
        ret.bufferBegin(bufcont);
        while (state != State.TERM && buf.readableByteCount() > 0 && buf.readableByteCount() >= needMin) {
            parse(buf, ret);
        }
        int bufEnd = buf.readPosition();
        if (inside == 1) {
            if (markBeg >= 0) {
                ret.add(markBeg, bufEnd - markBeg, ts, MapTsItemVec.Ty.OPEN);
            }
            else {
                ret.add(bufBeg, bufEnd - bufBeg, ts, MapTsItemVec.Ty.MIDDLE);
            }
        }
        if (state != State.TERM && buf.readableByteCount() > 0) {
            if (needMin <= 0) {
                throw new RuntimeException("logic");
            }
            if (buf.readableByteCount() >= needMin) {
                throw new RuntimeException("logic");
            }
            leftbufcont = BufCont.allocate(buf.factory(), 1024, BufCont.Mark.MapTs_keep_left);
            DataBuffer left = leftbufcont.bufferRef();
            left.write(buf);
        }
        buf.readPosition(bufBeg);
        buf.writePosition(bufEnd);
        bufcont.close();
        return ret;
    }

    void applyLeft(DataBuffer buf, MapTsItemVec ret) {
        DataBuffer left = leftbufcont.bufferRef();
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
        if (left.writableByteCount() < n) {
            throw new RuntimeException("logic");
        }
        left.write(buf.slice(buf.readPosition(), n));
        buf.readPosition(buf.readPosition() + n);
        if (buf.readableByteCount() + n != l1) {
            throw new RuntimeException("logic");
        }
        if (left.readableByteCount() != l2 + n) {
            throw new RuntimeException("logic");
        }
        leftCopiedTimes.getAndAdd(1);
        leftCopiedBytes.getAndAdd(n);
        if (left.readableByteCount() >= needMin) {
            markBeg = -1;
            bufBeg = left.readPosition();
            ret.bufferBegin(leftbufcont);
            parse(left, ret);
            if (left.readableByteCount() != 0) {
                throw new RuntimeException("logic");
            }
            if (inside == 1) {
                if (markBeg >= 0) {
                    ret.add(markBeg, left.readPosition() - markBeg, ts, MapTsItemVec.Ty.OPEN);
                }
                else {
                    ret.add(bufBeg, left.readPosition() - bufBeg, ts, MapTsItemVec.Ty.MIDDLE);
                }
            }
            left.writePosition(left.readPosition());
            left.readPosition(bufBeg);
            leftbufcont.close();
            leftbufcont = null;
        }
    }

    void parse(DataBuffer buf, MapTsItemVec res) {
        if (state == State.EXPECT_HEADER_A) {
            parseHeaderA(buf, res);
        }
        else if (state == State.EXPECT_BLOBS) {
            parseBlob(buf);
        }
        else if (state == State.EXPECT_SECOND_LENGTH) {
            parseSecondLength(buf, res);
        }
        else if (state == State.TERM) {
        }
        else {
            throw new RuntimeException("logic");
        }
    }

    void parseHeaderA(DataBuffer buf, MapTsItemVec res) {
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
        LOGGER.debug("{}  {}  seen  length  {}  timestamp {} {}  pulse {}", reqctx, name, length, ts / 1000000000L, ts % 1000000000, pulse);
        if (ts >= endNanos) {
            LOGGER.info("{}  {}  ts >= endNanos  {} {}  >=  {} {}", reqctx, name, ts / 1000000000L, ts % 1000000000, endNanos / 1000000000L, endNanos % 1000000000);
            state = State.TERM;
            buf.readPosition(bpos);
            buf.writePosition(bpos);
            return;
        }
        if (tsLast != Long.MIN_VALUE && ts < tsLast) {
            if (!detectedUnordered) {
                LOGGER.error("unordered ts  {}  {}  in {}", ts, tsLast, name);
                detectedUnordered = true;
            }
        }
        if (pulseLast != Long.MIN_VALUE && pulse < pulseLast) {
            if (false) {
                LOGGER.error("unordered pulse  {}  {}  in {}", pulse, pulseLast, name);
            }
        }
        tsLast = ts;
        pulseLast = pulse;
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
        state = State.EXPECT_BLOBS;
        missingBytes = length - needMin - 4;
        needMin = 0;
        inside = 1;
        markBeg = bpos;
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

    void parseSecondLength(DataBuffer buf, MapTsItemVec res) {
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
        if (markBeg >= 0) {
            res.add(markBeg, buf.readPosition() - markBeg, ts, MapTsItemVec.Ty.FULL);
        }
        else {
            res.add(bufBeg, buf.readPosition() - bufBeg, ts, MapTsItemVec.Ty.CLOSE);
        }
        state = State.EXPECT_HEADER_A;
        needMin = HEADER_A_LEN;
        inside = 0;
    }

    public synchronized void release() {
        if (closed) {
            LOGGER.error("EventBlobToV1MapTs already closed");
        }
        else {
            closeCount.getAndAdd(1);
        }
        LOGGER.debug("{}  {}  EventBlobToV1MapTs release", reqctx, name);
        if (leftbufcont != null) {
            BufCont k = leftbufcont;
            leftbufcont = null;
            k.close();
        }
    }

}
