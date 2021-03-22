package ch.psi.daq.retrieval.eventmap.ts;

import ch.psi.daq.retrieval.controller.ItemFilter;
import ch.psi.daq.retrieval.reqctx.BufCtx;
import ch.psi.daq.retrieval.reqctx.ReqCtx;
import ch.psi.daq.retrieval.bytes.BufCont;
import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
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
    static final AtomicLong pulseGapCount = new AtomicLong();
    static final Marker markBufTrace = ItemFilter.markBufTrace;
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
    ReqCtx reqCtx;
    boolean closed;
    boolean detectedUnordered;
    long tsLast = Long.MIN_VALUE;
    long pulseLast = Long.MIN_VALUE;
    int trailingEventsMax;
    int trailingEvents;
    boolean countGaps;

    enum State {
        EXPECT_HEADER_A,
        EXPECT_BLOBS,
        EXPECT_SECOND_LENGTH,
        TERM,
    }

    public EventBlobToV1MapTs(ReqCtx reqCtx, String name, long endNanos, int trailingEventsMax, BufCtx bufctx, boolean countGaps) {
        openCount.getAndAdd(1);
        this.reqCtx = reqCtx;
        this.name = name;
        this.endNanos = endNanos;
        this.trailingEventsMax = trailingEventsMax;
        this.state = State.EXPECT_HEADER_A;
        this.needMin = HEADER_A_LEN;
        this.countGaps = countGaps;
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
        public long pulseGapCount;
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
            pulseGapCount = EventBlobToV1MapTs.pulseGapCount.get();
        }
    }

    public static Flux<MapTsItemVec> trans(ReqCtx reqCtx, Flux<BufCont> fl, String name, String channelName, long endNanos, int trailingEvents, BufCtx bufCtx) {
        return trans(reqCtx, fl, name, channelName, endNanos, trailingEvents, bufCtx, false);
    }

    public static Flux<MapTsItemVec> trans(ReqCtx reqCtx, Flux<BufCont> fl, String name, String channelName, long endNanos, int trailingEvents, BufCtx bufCtx, boolean countGaps) {
        final EventBlobToV1MapTs mapper = new EventBlobToV1MapTs(reqCtx, name, endNanos, trailingEvents, bufCtx, countGaps);
        return fl
        .doOnDiscard(BufCont.class, BufCont::close)
        .map(mapper)
        .takeWhile(MapTsItemVec::notTerm)
        .doFinally(k -> mapper.release())
        .transform(doDiscard("EventBlobToV1MapTsTrans"))
        .doOnNext(obj -> obj.markWith(BufCont.Mark.MapTs_trans2));
    }

    @Override
    public synchronized MapTsItemVec apply(BufCont bufcont) {
        if (bufcont.isEmpty()) {
            LOGGER.info(markBufTrace, "{}  {}  apply  EMPTY", reqCtx, name);
            applyBufContEmpty.getAndAdd(1);
            bufcont.close();
            return MapTsItemVec.create();
        }
        else {
            final DataBuffer buf = bufcont.bufferRef();
            LOGGER.info(markBufTrace, "{}  {}  apply  rp {}  wp {}", reqCtx, name, buf.readPosition(), buf.writePosition());
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
    }

    MapTsItemVec applyInner(BufCont bufcont) {
        MapTsItemVec ret = MapTsItemVec.create();
        DataBuffer buf = bufcont.bufferRef();
        LOGGER.info(markBufTrace, "{}  {}  applyInner  rp {}  wp {}", reqCtx, name, buf.readPosition(), buf.writePosition());
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
                LOGGER.info(markBufTrace, "{}  OPEN    case A  {}", name, ts);
                ret.add(markBeg, bufEnd - markBeg, ts, pulse, MapTsItemVec.Ty.OPEN);
            }
            else {
                LOGGER.info(markBufTrace, "{}  MIDDLE  case A  {}", name, ts);
                ret.add(bufBeg, bufEnd - bufBeg, ts, pulse, MapTsItemVec.Ty.MIDDLE);
            }
        }
        if (state != State.TERM && buf.readableByteCount() > 0) {
            if (needMin <= 0) {
                throw new RuntimeException("logic");
            }
            if (buf.readableByteCount() >= needMin) {
                throw new RuntimeException("logic");
            }
            LOGGER.info(markBufTrace, "{}  {}  keep bytes left: {}", reqCtx, name, buf.readableByteCount());
            leftbufcont = BufCont.allocate(buf.factory(), 1024, BufCont.Mark.MapTs_keep_left);
            DataBuffer left = leftbufcont.bufferRef();
            left.write(buf);
        }
        buf.readPosition(bufBeg);
        buf.writePosition(bufEnd);
        LOGGER.info(markBufTrace, "{}  {}  ret stats {}", reqCtx, name, ret.stats());
        return ret;
    }

    void applyLeft(DataBuffer buf, MapTsItemVec ret) {
        LOGGER.info(markBufTrace, "{}  {}  applyLeft  rp {}  wp {}", reqCtx, name, buf.readPosition(), buf.writePosition());
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
            int bufEnd = left.readPosition();
            if (inside == 1) {
                if (markBeg >= 0) {
                    LOGGER.info(markBufTrace, "{}  OPEN    case B  {}", name, ts);
                    ret.add(markBeg, bufEnd - markBeg, ts, pulse, MapTsItemVec.Ty.OPEN);
                }
                else {
                    LOGGER.info(markBufTrace, "{}  MIDDLE  case A  {}", name, ts);
                    ret.add(bufBeg, bufEnd - bufBeg, ts, pulse, MapTsItemVec.Ty.MIDDLE);
                }
            }
            left.readPosition(bufBeg);
            left.writePosition(bufEnd);
            leftbufcont = null;
        }
    }

    void parse(DataBuffer buf, MapTsItemVec res) {
        if (state == State.EXPECT_HEADER_A) {
            parseHeaderA(buf);
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

    void parseHeaderA(DataBuffer buf) {
        int bpos = buf.readPosition();
        ByteBuffer bb = buf.asByteBuffer(buf.readPosition(), needMin);
        buf.readPosition(buf.readPosition() + needMin);
        int length = bb.getInt();
        if (length == 0) {
            LOGGER.warn("{}  {}  Stop because length == 0", reqCtx, name);
            state = State.TERM;
            return;
        }
        if (length < 40 || length > 60 * 1024 * 1024) {
            LOGGER.error("{}  {}  Stop because unexpected  length {}", reqCtx, name, length);
            state = State.TERM;
            return;
        }
        long ttl = bb.getLong();
        long ts = bb.getLong();
        long pulse = bb.getLong();
        long iocTime = bb.getLong();
        byte status = bb.get();
        byte severity = bb.get();
        LOGGER.info(markBufTrace, "{}  {}  headerA  pos {}  length  {}  timestamp {} {}  pulse {}", reqCtx, name, bpos, length, ts / 1000000000L, ts % 1000000000, pulse);
        if (ts >= endNanos) {
            if (trailingEvents >= trailingEventsMax) {
                LOGGER.debug("{}  {}  ts >= endNanos  {} {}  >=  {} {}", reqCtx, name, ts / 1000000000L, ts % 1000000000, endNanos / 1000000000L, endNanos % 1000000000);
                state = State.TERM;
                buf.readPosition(bpos);
                buf.writePosition(bpos);
                return;
            }
            else {
                trailingEvents += 1;
            }
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
        if (countGaps && pulseLast != Long.MIN_VALUE && pulseLast + 1 != pulse) {
            pulseGapCount.getAndAdd(1);
        }
        tsLast = ts;
        pulseLast = pulse;
        int optionalFieldsLength = bb.getInt();
        if (optionalFieldsLength < -1 || optionalFieldsLength == 0) {
            LOGGER.error("{}  {}  unexpected value for optionalFieldsLength: {}", reqCtx, name, optionalFieldsLength);
            throw new RuntimeException("unexpected optional fields");
        }
        if (optionalFieldsLength != -1) {
            LOGGER.warn("{}  {}  Found optional fields: {}", reqCtx, name, optionalFieldsLength);
        }
        if (optionalFieldsLength > 2048) {
            LOGGER.error("{}  {}  unexpected optional fields: {}", reqCtx, name, optionalFieldsLength);
            throw new RuntimeException("unexpected optional fields");
        }
        headerLength = needMin;
        this.ts = ts;
        this.pulse = pulse;
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
            LOGGER.warn("{}  {}  2nd length -1 encountered, ignoring", reqCtx, name);
        }
        else if (len2 == 0) {
            LOGGER.warn("{}  {}  2nd length 0 encountered, ignoring", reqCtx, name);
        }
        else if (len2 != blobLength) {
            LOGGER.error("{}  {}  event blob length mismatch at {}   {} vs {}", reqCtx, name, buf.readPosition(), len2, blobLength);
            throw new RuntimeException("unexpected 2nd length");
        }
        if (markBeg >= 0) {
            LOGGER.info(markBufTrace, "{}  FULL    case -  {}", name, ts);
            res.add(markBeg, buf.readPosition() - markBeg, ts, pulse, MapTsItemVec.Ty.FULL);
        }
        else {
            LOGGER.info(markBufTrace, "{}  CLOSE   case -  {}", name, ts);
            res.add(bufBeg, buf.readPosition() - bufBeg, ts, pulse, MapTsItemVec.Ty.CLOSE);
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
        LOGGER.debug("{}  {}  EventBlobToV1MapTs release", reqCtx, name);
        if (leftbufcont != null) {
            BufCont k = leftbufcont;
            leftbufcont = null;
            k.close();
        }
    }

}
