package ch.psi.daq.retrieval.eventmap.basic;

import ch.psi.daq.retrieval.reqctx.BufCtx;
import ch.psi.daq.retrieval.bytes.BufCont;
import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SynchronousSink;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class Basic implements Function<BufCont, BasicResult> {
    static final Logger LOGGER = (Logger) LoggerFactory.getLogger(Basic.class.getSimpleName());
    static final AtomicLong optionalFieldsFoundCount = new AtomicLong();
    static final int HEADER_A_LEN = 2 * Integer.BYTES + 4 * Long.BYTES + 2 * Byte.BYTES;
    BasicParams params;
    BufCont leftCont;
    State state;
    int needMin;
    int blobsType = -1;
    int blobsArray = -1;
    int blobsShape = -1;
    int blobsCompression = -1;
    int shapeDims = -1;
    int missingBytes = -1;
    int valueBytes = -1;
    int[] shapeLens = new int[4];
    ByteOrder blobsByteOrder = null;
    long pulse = -1;
    long ts = -1;
    int blobLength = -1;
    int headerLength = -1;
    int optionalFieldsLength = -1;
    int resCap = 8;
    long seenHeaderA;

    enum State {
        EXPECT_HEADER_A,
        EXPECT_HEADER_B,
        EXPECT_HEADER_C,
        EXPECT_HEADER_D,
        EXPECT_BLOBS,
        EXPECT_SECOND_LENGTH,
        TERM,
    }

    public Basic(BasicParams params) {
        this.params = params;
        this.state = State.EXPECT_HEADER_A;
        this.needMin = HEADER_A_LEN;
    }

    public static Flux<BasicResult> trans(Flux<BufCont> fl, BasicParams params) {
        Basic mapper = new Basic(params);
        return fl.map(mapper)
        //.concatWith(Mono.defer(() -> Mono.just(mapper.lastResult())))
        .takeWhile(item -> {
            if (item.notTerm()) {
                return true;
            }
            else {
                item.release();
                return false;
            }
        })
        .doFinally(k -> mapper.release());
    }

    public static Flux<BufCont> transDesc(Flux<BasicResult> fl, BufCtx bufCtx) {
        return fl.concatMap(k -> Flux.generate(() -> new Iter(bufCtx, k), Iter::next, Iter::release));
    }

    static class Iter {
        Iter(BufCtx bufCtx, BasicResult res) {
            this.bufCtx = bufCtx;
            this.res = res;
        }
        Iter next(SynchronousSink<BufCont> sink) {
            if (ix >= res.chunks.size()) {
                sink.complete();
            }
            else {
                BufCont bc = BufCont.allocate(bufCtx.bufFac, bufCtx.bufferSize, BufCont.Mark.__NOMARK);
                bc.bufferRef().write(res.chunks.get(ix).desc(), StandardCharsets.UTF_8);
                bc.bufferRef().write((byte) 10);
                ix += 1;
                sink.next(bc);
            }
            return this;
        }
        void release() {
            res.release();
        }
        BufCtx bufCtx;
        BasicResult res;
        int ix;
    }

    @Override
    public BasicResult apply(BufCont bufCont) {
        if (bufCont.isEmpty()) {
            throw new RuntimeException("empty BufCont");
        }
        else {
            DataBuffer b = bufCont.bufferRef();
            //LOGGER.info("apply  bufCont {}  {}  {}  {}", bufCont.readableByteCount(), b.readPosition(), b.writePosition(), b.capacity());
            if (state == State.TERM) {
                bufCont.close();
                return BasicResult.term();
            }
            else {
                return applyBuf(bufCont);
            }
        }
    }

    BasicResult applyBuf(BufCont bufCont) {
        final DataBuffer buf = bufCont.bufferRef();
        final BasicResult ret = BasicResult.create(resCap);
        if (leftCont != null) {
            if (!leftCont.hasBuf()) {
                leftCont.close();
                leftCont = null;
            }
            else {
                applyLeft(ret, buf);
            }
        }
        ret.bufBegin(bufCont);
        while (state != State.TERM && buf.readableByteCount() >= needMin) {
            parse(ret, buf);
        }
        if (state != State.TERM && buf.readableByteCount() > 0) {
            if (needMin <= 0) {
                throw new RuntimeException("logic");
            }
            if (buf.readableByteCount() >= needMin) {
                throw new RuntimeException("logic");
            }
            leftCont = bufCont;
        }
        else {
            bufCont.close();
        }
        return ret;
    }

    void applyLeft(BasicResult res, DataBuffer buf) {
        DataBuffer left = leftCont.bufferRef();
        if (needMin <= 0) {
            throw new RuntimeException("logic");
        }
        else if (left.readableByteCount() <= 0) {
            throw new RuntimeException("logic");
        }
        else if (left.readableByteCount() >= needMin) {
            throw new RuntimeException("logic");
        }
        else {
            int n = Math.min(buf.readableByteCount(), needMin - left.readableByteCount());
            int l1 = buf.readableByteCount();
            int l2 = left.readableByteCount();
            left.write(buf.slice(buf.readPosition(), n));
            buf.readPosition(buf.readPosition() + n);
            if (buf.readableByteCount() + n != l1) {
                throw new RuntimeException("logic");
            }
            else if (left.readableByteCount() != l2 + n) {
                throw new RuntimeException("logic");
            }
            else if (left.readableByteCount() < needMin) {
                // TODO ?
            }
            else {
                LOGGER.debug("parse left  {}", state);
                res.bufBegin(leftCont);
                parse(res, left);
                if (left.readableByteCount() != 0) {
                    leftCont.close();
                    leftCont = null;
                    throw new RuntimeException("logic");
                }
                leftCont.close();
                leftCont = null;
            }
        }
    }

    void parse(BasicResult res, DataBuffer buf) {
        if (buf == null) {
            throw new RuntimeException("logic");
        }
        if (state == State.EXPECT_HEADER_A) {
            parseHeaderA(res, buf);
        }
        else if (state == State.EXPECT_HEADER_B) {
            parseHeaderB(res, buf);
        }
        else if (state == State.EXPECT_HEADER_C) {
            parseHeaderC(res, buf);
        }
        else if (state == State.EXPECT_HEADER_D) {
            parseHeaderD(res, buf);
        }
        else if (state == State.EXPECT_BLOBS) {
            parseBlob(res, buf);
        }
        else if (state == State.EXPECT_SECOND_LENGTH) {
            parseSecondLength(res, buf);
        }
        else if (state == State.TERM) {
        }
        else {
            // TODO count
        }
    }

    void parseHeaderA(BasicResult res, DataBuffer buf) {
        final int rp = buf.readPosition();
        final int len = needMin;
        final ByteBuffer bb = buf.asByteBuffer(buf.readPosition(), needMin);
        buf.readPosition(buf.readPosition() + needMin);
        final int length = bb.getInt();
        if (length == 0) {
            LOGGER.info("Stop because length == 0");
            state = State.TERM;
            return;
        }
        if (length < 0 || length > 30 * 1024 * 1024) {
            LOGGER.error("{}  Stop because unexpected length {}", params.name, length);
            state = State.TERM;
            return;
        }
        long ttl = bb.getLong();
        long ts = bb.getLong();
        long pulse = bb.getLong();
        long iocTime = bb.getLong();
        byte status = bb.get();
        byte severity = bb.get();
        seenHeaderA += 1;
        if (false && seenHeaderA > 1000) {
            LOGGER.error("break after {} headers", seenHeaderA);
            state = State.TERM;
            return;
        }
        //LOGGER.info("seen  length {}  timestamp {}  pulse {}  seenHeaderA {}", length, ts, pulse, seenHeaderA);
        if (ts >= params.endNanos) {
            LOGGER.info("{}  stop  ts {}  >=  end {}", params.name, ts, params.endNanos);
            state = State.TERM;
            return;
        }
        int optionalFieldsLength = bb.getInt();
        if (optionalFieldsLength < -1 || optionalFieldsLength == 0) {
            LOGGER.error("{}  unexpected value for optionalFieldsLength {}", params.name, optionalFieldsLength);
            throw new RuntimeException("unexpected optional fields");
        }
        if (optionalFieldsLength != -1) {
            optionalFieldsFoundCount.getAndAdd(1);
        }
        if (optionalFieldsLength > 2048) {
            LOGGER.error("{}  unexpected optional fields: {}", params.name, optionalFieldsLength);
            throw new RuntimeException("unexpected optional fields");
        }
        headerLength = HEADER_A_LEN;
        res.add(new ChunkHeaderA(rp, len, ts, pulse));
        this.pulse = pulse;
        this.ts = ts;
        this.optionalFieldsLength = Math.max(optionalFieldsLength, 0);
        this.blobLength = length;
        state = State.EXPECT_HEADER_B;
        needMin = this.optionalFieldsLength + Short.BYTES;
    }

    void parseHeaderB(BasicResult res, DataBuffer buf) {
        final int rp = buf.readPosition();
        final int len = needMin;
        ByteBuffer bb = buf.asByteBuffer(rp, len);
        buf.readPosition(rp + len);
        if (bb.position() != 0) {
            LOGGER.warn("readHeaderB test position {}", bb.position());
        }
        headerLength += len;
        bb.position(bb.position() + optionalFieldsLength);
        needMin = 0;
        int dtypeBitmask = 0xffff & bb.getShort();
        blobsType = 0xff & dtypeBitmask;
        if (blobsType > 13) {
            LOGGER.error("unexpected datatype: {}", blobsType);
            throw new RuntimeException("unexpected datatype");
        }
        if ((dtypeBitmask & 0x8000) != 0) {
            if (blobsCompression != -1 && blobsCompression != 1) {
                throw new RuntimeException("logic");
            }
            blobsCompression = 1;
            needMin += Byte.BYTES;
        }
        else {
            if (blobsCompression != -1 && blobsCompression != 0) {
                throw new RuntimeException("logic");
            }
            blobsCompression = 0;
        }
        if ((dtypeBitmask & 0x4000) != 0) {
            if (blobsArray != -1 && blobsArray != 1) {
                throw new RuntimeException("logic");
            }
            blobsArray = 1;
        }
        else {
            if (blobsArray != -1 && blobsArray != 0) {
                throw new RuntimeException("logic");
            }
            blobsArray = 0;
        }
        if ((dtypeBitmask & 0x2000) != 0) {
            if (blobsByteOrder != null && blobsByteOrder != ByteOrder.BIG_ENDIAN) {
                throw new RuntimeException("inhomogeneous endianness");
            }
            blobsByteOrder = ByteOrder.BIG_ENDIAN;
        }
        else {
            if (blobsByteOrder != null && blobsByteOrder != ByteOrder.LITTLE_ENDIAN) {
                throw new RuntimeException("inhomogeneous endianness");
            }
            blobsByteOrder = ByteOrder.LITTLE_ENDIAN;
        }
        //LOGGER.info(String.format("dtypeBitmask: %8x", dtypeBitmask));
        if ((dtypeBitmask & 0x1000) != 0) {
            if (blobsShape != -1 && blobsShape != 1) {
                throw new RuntimeException("logic");
            }
            blobsShape = 1;
            needMin += Byte.BYTES;
        }
        else {
            if (blobsShape != -1 && blobsShape != 0) {
                throw new RuntimeException("logic");
            }
            blobsShape = 0;
            shapeDims = 0;
        }
        if (((dtypeBitmask & 0x4000) != 0) && ((dtypeBitmask & 0x1000) == 0)) {
            //throw new RuntimeException("array without shape");
            // This is a writer bug. Lots of archived data already has this.
        }
        if (((dtypeBitmask & 0x1000) != 0) && ((dtypeBitmask & 0x4000) == 0)) {
            throw new RuntimeException("shape without array");
        }
        res.add(new ChunkHeaderB(rp, len));
        state = State.EXPECT_HEADER_C;
    }

    void parseHeaderC(BasicResult res, DataBuffer buf) {
        final int rp = buf.readPosition();
        final int len = needMin;
        ByteBuffer bb = buf.asByteBuffer(rp, len);
        buf.readPosition(rp + len);
        headerLength += needMin;
        needMin = 0;
        if (blobsCompression == 1) {
            byte b = bb.get();
            if (b == 0) {
                // bitshuffle lz4
                blobsCompression = 1;
            }
            else if (b == 1) {
                // lz4
                blobsCompression = 2;
            }
            else {
                LOGGER.error("unknown compressionMethod: {}  pulse: {}", b, pulse);
            }
        }
        if (blobsShape == 1) {
            shapeDims = 0xff & bb.get();
            needMin = shapeDims * Integer.BYTES;
            LOGGER.debug("has shape, dims: {}", shapeDims);
        }
        if (shapeDims > 3) {
            //LOGGER.info("currentEvent.shapeDims  {} > 3", shapeDims);
            throw new RuntimeException("unexpected shapeDims");
        }
        res.add(new ChunkHeaderC(rp, len));
        state = State.EXPECT_HEADER_D;
    }

    void parseHeaderD(BasicResult res, DataBuffer buf) {
        final int rp = buf.readPosition();
        final int len = needMin;
        ByteBuffer bb = buf.asByteBuffer(rp, len);
        buf.readPosition(rp + len);
        headerLength += needMin;
        valueBytes = blobLength - headerLength - Integer.BYTES;
        if (valueBytes < 1 || valueBytes > 20 * 1024 * 1024) {
            throw new RuntimeException(String.format("%s  unexpected  valueBytes %d", params.name, valueBytes));
        }
        missingBytes = valueBytes;
        LOGGER.trace("blobLength {}  headerLength {}  missing {}", blobLength, headerLength, valueBytes);
        for (int i = 0; i < shapeDims; i++) {
            int n = bb.getInt();
            shapeLens[i] = n;
        }
        res.add(new ChunkHeaderD(rp, len));
        state = State.EXPECT_BLOBS;
        needMin = 1;
        // ugly fixes for bug in writer
        if (blobsCompression > 0) {
            needMin = 8;
        }
    }

    void parseBlob(BasicResult res, DataBuffer buf) {
        final int rp = buf.readPosition();
        final int len = Math.min(missingBytes, buf.readableByteCount());
        buf.readPosition(rp + len);
        missingBytes -= len;
        res.add(new ChunkBlob(rp, len));
        if (missingBytes > 0) {
            needMin = 1;
        }
        else {
            state = State.EXPECT_SECOND_LENGTH;
            needMin = Integer.BYTES;
        }
    }

    void parseSecondLength(BasicResult res, DataBuffer buf) {
        final int rp = buf.readPosition();
        final int len = needMin;
        ByteBuffer bb = buf.asByteBuffer(rp, len);
        buf.readPosition(rp + len);
        int len2 = bb.getInt();
        if (len2 != -1 && len2 != 0 && len2 != blobLength) {
            LOGGER.error("event blob length mismatch  seenHeaderA {}  at {}  len2 {}  blobLength {}  pulse {}", seenHeaderA, buf.readPosition(), len2, blobLength, pulse);
            throw new RuntimeException("unexpected 2nd length");
        }
        res.add(new ChunkLen2(rp, len));
        state = State.EXPECT_HEADER_A;
        needMin = HEADER_A_LEN;
    }

    public void release() {
        if (leftCont != null) {
            BufCont bc = leftCont;
            leftCont = null;
            bc.close();
        }
    }

}
