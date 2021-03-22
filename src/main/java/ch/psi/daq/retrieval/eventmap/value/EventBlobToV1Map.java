package ch.psi.daq.retrieval.eventmap.value;

import ch.psi.bitshuffle.BitShuffleLZ4JNIDecompressor;
import ch.psi.daq.retrieval.reqctx.BufCtx;
import ch.psi.daq.retrieval.utils.DTypeBitmapUtils;
import ch.psi.daq.retrieval.QueryParams;
import ch.psi.daq.retrieval.reqctx.ReqCtx;
import ch.psi.daq.retrieval.bytes.BufCont;
import ch.psi.daq.retrieval.bytes.Output;
import ch.psi.daq.retrieval.merger.Releasable;
import ch.qos.logback.classic.Logger;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.jpountz.lz4.LZ4Factory;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static ch.psi.daq.retrieval.controller.QueryData.doDiscard;

public class EventBlobToV1Map implements Function<BufCont, EventBlobMapResult>, Releasable {
    static final Logger LOGGER = (Logger) LoggerFactory.getLogger(EventBlobToV1Map.class.getSimpleName());
    static final int HEADER_A_LEN = 2 * Integer.BYTES + 4 * Long.BYTES + 2 * Byte.BYTES;
    static final AtomicLong leftCopiedTimes = new AtomicLong();
    static final AtomicLong leftCopiedBytes = new AtomicLong();
    static final AtomicLong applyBufContEmpty = new AtomicLong();
    static final AtomicLong applyDespiteTerm = new AtomicLong();
    static final AtomicLong applyEmptyBuffer = new AtomicLong();
    static final AtomicLong leftWithoutBuf = new AtomicLong();
    static final AtomicLong pulseGapCount = new AtomicLong();
    DataBufferFactory bufFac;
    BufCont kbufcont = BufCont.makeEmpty(BufCont.Mark.V1Map_init);
    BufCont leftbufcont;
    boolean headerOut;
    boolean blobHeaderOut;
    int bufferSize;
    int bufferSize2;
    String channelName;
    State state;
    int needMin;
    int blobsType = -1;
    int blobsArray = -1;
    int blobsShape = -1;
    int blobsCompression = -1;
    int shapeDims = -1;
    int[] shapeLens;
    int shapeDimsHeader;
    int[] shapeLensHeader = new int[4];
    int missingBytes;
    int valueBytes;
    ByteOrder blobsByteOrder = null;
    long pulse;
    long ts;
    boolean detectedUnordered;
    long tsLast = Long.MIN_VALUE;
    long pulseLast = Long.MIN_VALUE;
    int blobLength;
    int headerLength;
    int optionalFieldsLength;
    long endNanos;
    boolean unpackOnServer;
    boolean blobUnpack;
    QueryParams qp;
    long writtenEvents;
    long writtenBytes;
    long seenHeaderA;
    ReqCtx reqctx;
    int dtNum = Integer.MIN_VALUE;
    int dtFlags = Integer.MIN_VALUE;
    AtomicInteger released = new AtomicInteger();

    enum State {
        EXPECT_HEADER_A,
        EXPECT_HEADER_B,
        EXPECT_HEADER_C,
        EXPECT_HEADER_D,
        EXPECT_BLOBS,
        EXPECT_SECOND_LENGTH,
        TERM,
    }

    @JsonPropertyOrder(alphabetic = true)
    static class BlobJsonHeader {
        @JsonInclude(JsonInclude.Include.NON_NULL)
        public String name;
        @JsonInclude(JsonInclude.Include.NON_NULL)
        public String type;
        @JsonInclude(JsonInclude.Include.NON_NULL)
        public String compression;
        boolean array;
        @JsonInclude(JsonInclude.Include.NON_NULL)
        public String byteOrder;
        @JsonInclude(JsonInclude.Include.NON_NULL)
        public List<Integer> shape;
    }

    public static class Stats {
        public long leftCopiedTimes;
        public long leftCopiedBytes;
        public long applyBufContEmpty;
        public long applyDespiteTerm;
        public long applyEmptyBuffer;
        public long leftWithoutBuf;
        public long pulseGapCount;
        public long resultTakenEmpty;
        public Stats() {
            leftCopiedTimes = EventBlobToV1Map.leftCopiedTimes.get();
            leftCopiedBytes = EventBlobToV1Map.leftCopiedBytes.get();
            applyBufContEmpty = EventBlobToV1Map.applyBufContEmpty.get();
            applyDespiteTerm = EventBlobToV1Map.applyDespiteTerm.get();
            applyEmptyBuffer = EventBlobToV1Map.applyEmptyBuffer.get();
            leftWithoutBuf = EventBlobToV1Map.leftWithoutBuf.get();
            pulseGapCount = EventBlobToV1Map.pulseGapCount.get();
            resultTakenEmpty = EventBlobMapResult.takenEmpty.get();
        }
    }

    public EventBlobToV1Map(ReqCtx reqctx, String channelName, long endNanos, DataBufferFactory bufFac, int bufferSize, QueryParams qp) {
        this.reqctx = reqctx;
        this.channelName = channelName;
        this.bufFac = bufFac;
        this.bufferSize = bufferSize;
        this.bufferSize2 = bufferSize * 2;
        this.endNanos = endNanos;
        this.qp = qp;
        this.unpackOnServer = qp.decompressOnServer;
        this.unpackOnServer = false;
        this.state = State.EXPECT_HEADER_A;
        this.needMin = HEADER_A_LEN;
    }

    public static Flux<EventBlobMapResult> trans(ReqCtx reqctx, Flux<BufCont> fl, String channelName, long endNanos, BufCtx bufCtx, QueryParams qp) {
        final EventBlobToV1Map mapper = new EventBlobToV1Map(reqctx, channelName, endNanos, bufCtx.bufFac, bufCtx.bufferSize, qp);
        return fl.map(mapper)
        .concatWith(Mono.defer(() -> Mono.just(mapper.lastResult())))
        .takeWhile(EventBlobMapResult::notTerm)
        .transform(doDiscard("EventBlobToV1MapTrans"))
        .doFinally(k -> mapper.release());
    }

    @Override
    public synchronized EventBlobMapResult apply(BufCont bufcont) {
        if (bufcont.isEmpty()) {
            applyBufContEmpty.getAndAdd(1);
            bufcont.close();
            return EventBlobMapResult.empty();
        }
        if (state == State.TERM) {
            applyDespiteTerm.getAndAdd(1);
            bufcont.close();
            return EventBlobMapResult.term();
        }
        final DataBuffer buf = bufcont.bufferRef();
        if (buf.readableByteCount() == 0) {
            applyEmptyBuffer.getAndAdd(1);
        }
        Output out = new Output(bufFac, bufferSize2, BufCont.Mark.V1Map);
        if (leftbufcont != null && !leftbufcont.hasBuf()) {
            leftWithoutBuf.getAndAdd(1);
            leftbufcont.close();
            leftbufcont = null;
        }
        if (leftbufcont != null && leftbufcont.hasBuf()) {
            applyLeft(buf, out);
        }
        int bufBeg = buf.readPosition();
        while (state != State.TERM && buf.readableByteCount() > 0 && buf.readableByteCount() >= needMin) {
            parse(buf, out);
        }
        int bufEnd = buf.readPosition();
        if (state != State.TERM && buf.readableByteCount() > 0) {
            if (needMin <= 0) {
                throw new RuntimeException("logic");
            }
            if (buf.readableByteCount() >= needMin) {
                throw new RuntimeException("logic");
            }
            leftbufcont = BufCont.allocate(buf.factory(), 1024, BufCont.Mark.V1Map_left_alloc);
            DataBuffer left = leftbufcont.bufferRef();
            left.write(buf);
        }
        buf.readPosition(bufBeg);
        buf.writePosition(bufEnd);
        bufcont.close();
        return EventBlobMapResult.fromBuffers(out.take());
    }

    void applyLeft(DataBuffer buf, Output out) {
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
            int bufBeg = left.readPosition();
            parse(left, out);
            if (left.readableByteCount() != 0) {
                throw new RuntimeException("logic");
            }
            left.writePosition(left.readPosition());
            left.readPosition(bufBeg);
            leftbufcont.close();
            leftbufcont = null;
        }
    }

    void parse(DataBuffer buf, Output out) {
        LOGGER.trace("{}  parse  state: {}  buf: {}", reqctx, state, buf);
        if (buf == null) {
            throw new RuntimeException("logic");
        }
        if (state == State.EXPECT_HEADER_A) {
            parseHeaderA(buf);
        }
        else if (state == State.EXPECT_HEADER_B) {
            parseHeaderB(buf);
        }
        else if (state == State.EXPECT_HEADER_C) {
            parseHeaderC(buf);
        }
        else if (state == State.EXPECT_HEADER_D) {
            parseHeaderD(buf);
        }
        else if (state == State.EXPECT_BLOBS) {
            parseBlob(buf, out);
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
        ByteBuffer bb = buf.asByteBuffer(buf.readPosition(), needMin);
        buf.readPosition(buf.readPosition() + needMin);

        blobHeaderOut = false;
        int length = bb.getInt();
        if (length == 0) {
            LOGGER.info("{}  header A top because length == 0", reqctx);
            state = State.TERM;
            return;
        }

        if (length < 0 || length > 60 * 1024 * 1024) {
            LOGGER.error("{}  stop because unexpected length {}", reqctx, length);
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
        LOGGER.trace("{}  seen  length {}  timestamp {}  pulse {}  seenHeaderA {}", reqctx, length, ts, pulse, seenHeaderA);
        if (ts >= endNanos) {
            LOGGER.debug("{}  stop  ts {}  >=  end {}", reqctx, ts, endNanos);
            state = State.TERM;
            return;
        }
        if (tsLast != Long.MIN_VALUE && ts < tsLast) {
            if (!detectedUnordered) {
                LOGGER.error("unordered ts  {}  {}  in {}", ts, tsLast, channelName);
                detectedUnordered = true;
            }
        }
        if (pulseLast != Long.MIN_VALUE && pulseLast + 1 != pulse) {
            pulseGapCount.getAndAdd(1);
        }
        tsLast = ts;
        pulseLast = pulse;
        int optionalFieldsLength = bb.getInt();
        if (optionalFieldsLength < -1 || optionalFieldsLength == 0) {
            LOGGER.error("{}  unexpected value for optionalFieldsLength: {}", reqctx, optionalFieldsLength);
            throw new RuntimeException("unexpected optional fields");
        }
        if (optionalFieldsLength != -1) {
            LOGGER.warn("{}  Found optional fields: {}", reqctx, optionalFieldsLength);
        }
        if (optionalFieldsLength > 2048) {
            LOGGER.error("{}  unexpected optional fields: {}", reqctx, optionalFieldsLength);
            throw new RuntimeException("unexpected optional fields");
        }
        headerLength = HEADER_A_LEN;
        this.pulse = pulse;
        this.ts = ts;
        this.optionalFieldsLength = Math.max(optionalFieldsLength, 0);
        this.blobLength = length;
        if (blobLength < 1 || blobLength > 20 * 1024 * 1024) {
            LOGGER.error("{}  Unexpected blobLength {}", reqctx, blobLength);
            throw new RuntimeException("Unexpected blobLength");
        }
        state = State.EXPECT_HEADER_B;
        needMin = this.optionalFieldsLength + Short.BYTES;
    }

    void parseHeaderB(DataBuffer buf) {
        ByteBuffer bb = buf.asByteBuffer(buf.readPosition(), needMin);
        buf.readPosition(buf.readPosition() + needMin);
        if (bb.position() != 0) {
            LOGGER.warn("{}  readHeaderB test position {}", reqctx, bb.position());
        }
        headerLength += needMin;
        bb.position(bb.position() + optionalFieldsLength);
        needMin = 0;
        int j = 0xffff & bb.getShort();
        int dtypeFlags = j >> 8;
        int dtypeNum = 0xff & j;
        if (dtNum == Integer.MIN_VALUE) {
            dtNum = dtypeNum;
            dtFlags = dtypeFlags;
            blobsType = dtypeNum;
        }
        else {
            if (dtypeNum != dtNum) {
                throw new RuntimeException(String.format("change of dtNum  old %d  new %d", dtNum, dtypeNum));
            }
            if (dtypeFlags != dtFlags) {
                throw new RuntimeException(String.format("change of dtFlags  old %d  new %d", dtFlags, dtypeFlags));
            }
        }
        if (blobsType > 13) {
            LOGGER.error("{}  unexpected datatype: {}", reqctx, blobsType);
            throw new RuntimeException("unexpected datatype");
        }
        if ((dtypeFlags & 0x80) != 0) {
            if (blobsCompression != -1 && blobsCompression != 1) {
                throw new RuntimeException("logic");
            }
            blobsCompression = 1;
            if (unpackOnServer) {
                blobUnpack = true;
            }
            needMin += Byte.BYTES;
        }
        else {
            if (blobsCompression != -1 && blobsCompression != 0) {
                throw new RuntimeException("logic");
            }
            blobUnpack = false;
            blobsCompression = 0;
        }
        if ((dtypeFlags & 0x40) != 0) {
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
        if ((dtypeFlags & 0x20) != 0) {
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
        if ((dtypeFlags & 0x10) != 0) {
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
        if (((dtypeFlags & 0x40) != 0) && ((dtypeFlags & 0x10) == 0)) {
            //throw new RuntimeException("array without shape");
            // This is a writer bug. Lots of archived data already has this.
        }
        if (((dtypeFlags & 0x10) != 0) && ((dtypeFlags & 0x40) == 0)) {
            throw new RuntimeException("shape without array");
        }
        state = State.EXPECT_HEADER_C;
    }

    void parseHeaderC(DataBuffer buf) {
        ByteBuffer bb = buf.asByteBuffer(buf.readPosition(), needMin);
        buf.readPosition(buf.readPosition() + needMin);
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
                LOGGER.error("{}  unknown compressionMethod: {}  pulse: {}  {}", reqctx, b, pulse, channelName);
            }
        }
        if (blobsShape == 1) {
            int n = 0xff & bb.get();
            if (shapeDims == -1) {
                shapeDims = n;
                shapeDimsHeader = n;
            }
            else {
                if (n != shapeDims) {
                    throw new RuntimeException(String.format("change in shapeDims  old %d  new %d", shapeDims, n));
                }
            }
            needMin = shapeDims * Integer.BYTES;
        }
        if (shapeDims > 3) {
            LOGGER.warn("{}  currentEvent.shapeDims  {}  {} > 3", reqctx, channelName, shapeDims);
            throw new RuntimeException("unexpected shapeDims");
        }
        state = State.EXPECT_HEADER_D;
    }

    void parseHeaderD(DataBuffer buf) {
        ByteBuffer bb = buf.asByteBuffer(buf.readPosition(), needMin);
        buf.readPosition(buf.readPosition() + needMin);
        headerLength += needMin;
        valueBytes = blobLength - headerLength - Integer.BYTES;
        if (valueBytes < 1 || valueBytes > 20 * 1024 * 1024) {
            throw new RuntimeException(String.format("Unexpected  valueBytes %d", valueBytes));
        }
        missingBytes = valueBytes;
        LOGGER.trace("{}  blobLength {}  headerLength {}  missing {}", reqctx, blobLength, headerLength, valueBytes);
        if (shapeLens == null) {
            shapeLens = new int[shapeDims];
            for (int i = 0; i < shapeDims; i++) {
                int n = bb.getInt();
                shapeLens[i] = n;
                shapeLensHeader[i] = n;
            }
        }
        else {
            for (int i = 0; i < shapeDims; i++) {
                int n = bb.getInt();
                if (n != shapeLens[i]) {
                    throw new RuntimeException(String.format("shape change  i %d  old %d  new %d", i, shapeLens[i], n));
                }
            }
        }
        if (blobUnpack) {
            LOGGER.error("{}  blobUnpack not supported currently", reqctx);
            throw new RuntimeException("todo");
        }
        state = State.EXPECT_BLOBS;
        needMin = 0;
        // ugly fixes for bug in writer
        if (blobsCompression > 0 || blobsArray == 1 && blobsShape != 1) {
            needMin = 8;
        }
    }

    int sizeOf(int type) {
        if (type == 12) return 8;
        if (type == 11) return 4;
        if (type == 10) return 8;
        if (type ==  9) return 8;
        if (type ==  8) return 4;
        if (type ==  7) return 4;
        if (type ==  5) return 2;
        if (type ==  4) return 2;
        return 1;
    }

    void parseBlob(DataBuffer buf, Output out) {
        final int n = Math.min(buf.readableByteCount(), missingBytes);
        // TODO do we enforce same compression for all blobs?
        if (blobUnpack) {
            if (true) {
                LOGGER.error("{}  blobUnpack not supported currently", reqctx);
                throw new RuntimeException("todo");
            }
            if (kbufcont.isEmpty()) {
                kbufcont = BufCont.allocate(bufFac, n, BufCont.Mark.V1Map_kbuf_alloc);
            }
            // TODO
            //kbufcont = kbufcont.ensureWritable(n);
            kbufcont.bufferRef().write(buf.slice(buf.readPosition(), n));
            buf.readPosition(buf.readPosition() + n);
        }
        else {
            if (!blobHeaderOut) {
                if (blobsArray == 1 && blobsShape != 1) {
                    if (buf.readableByteCount() < 8) {
                        throw new RuntimeException("logic");
                    }
                    ByteBuffer src = buf.asByteBuffer(buf.readPosition(), 8);
                    int nf = (int) src.getLong();
                    //LOGGER.info("{}  nf {}  shapeDims {}  shapeLens {}", reqctx, nf, shapeDims, shapeLens);
                    shapeDimsHeader = 1;
                    shapeLensHeader[0] = nf / sizeOf(blobsType);
                }
                if (!headerOut) {
                    writeHeader(out, channelName);
                    headerOut = true;
                }
                out.ensureWritable(21);
                DataBuffer cbuf = out.bufferRef();
                ByteBuffer bbuf = cbuf.asByteBuffer(cbuf.writePosition(), 21);
                cbuf.writePosition(cbuf.writePosition() + 21);
                bbuf.putInt(17 + valueBytes);
                bbuf.put((byte) 1);
                bbuf.putLong(ts);
                bbuf.putLong(pulse);
                blobHeaderOut = true;
            }
            out.transfer(buf, n);
        }
        missingBytes -= n;
        if (missingBytes > 0) {
            needMin = 0;
        }
        else {
            if (blobUnpack) {
                if (true) {
                    LOGGER.error("{}  blobUnpack not supported currently", reqctx);
                    throw new RuntimeException("todo");
                }
                final DataBuffer kbuf = kbufcont.bufferRef();
                if (blobsCompression == 1) {
                    if (false) {
                        ByteBuffer src = kbuf.asByteBuffer(12, kbuf.writePosition());
                        ByteBuffer dst = ByteBuffer.allocate(8 * 1024);
                        LZ4Factory.safeInstance().safeDecompressor().decompress(src, dst);
                    }
                    ByteBuffer src = kbuf.asByteBuffer(0, kbuf.writePosition());
                    ByteBuffer dst = ByteBuffer.allocate((20 * kbuf.readableByteCount() / 1024 + 1) * 1024);
                    BitShuffleLZ4JNIDecompressor decompressor = new BitShuffleLZ4JNIDecompressor();
                    // try it similar to the python implementation:
                    int nf = (int) src.getLong();
                    if (nf > 2 * 1024 * 1024) {
                        LOGGER.error("{}  large compressed waveform  channel: {}  pulse: {}  nf: {}", reqctx, channelName, pulse, nf);
                        throw new RuntimeException("large waveform");
                    }
                    int bs = src.getInt();
                    LOGGER.trace("{}  pulse: {}  n: {}  nf: {}  bs: {}  ty: {}  order: {}", reqctx, pulse, n, nf, bs, blobsType, blobsByteOrder);
                    int tsize = sizeOf(blobsType);
                    int a = decompressor.decompress(src, 12, dst, 0, nf/tsize, tsize, bs);
                    dst.limit(nf);
                    if (false) {
                        for (int i1 = 0; i1 < 8; i1 += 1) {
                            System.err.println(String.format("i1: %3d  v: %10d", i1, dst.getInt()));
                        }
                    }
                    dst.position(0);
                    if (!headerOut) {
                        // ugly fixes for bug in writer
                        if (blobsArray == 1 && blobsShape != 1) {
                            //LOGGER.info("current shape: {}  {}", shapeDims, shapeLens);
                            shapeDims = 1;
                            shapeLens[0] = nf/tsize;
                        }
                        writeHeader(out, channelName);
                        headerOut = true;
                    }
                    {
                        int m = nf;
                        out.ensureWritable(21 + dst.remaining() + 4);
                        DataBuffer cbuf = out.bufferRef();
                        ByteBuffer bbuf = cbuf.asByteBuffer(cbuf.writePosition(), 21);
                        cbuf.writePosition(cbuf.writePosition() + 21);
                        bbuf.putInt(17 + m);
                        bbuf.put((byte) 1);
                        bbuf.putLong(ts);
                        bbuf.putLong(pulse);
                        cbuf.write(dst);
                        bbuf = cbuf.asByteBuffer(cbuf.writePosition(), 4);
                        cbuf.writePosition(cbuf.writePosition() + 4);
                        bbuf.putInt(17 + m);
                    }
                }
                else if (blobsCompression == 2) {
                    throw new RuntimeException("plain lz4 not handled yet");
                }
                else {
                    throw new RuntimeException("unknown compression");
                }
                kbuf.readPosition(0);
                kbuf.writePosition(0);
            }
            else {
                out.ensureWritable(4);
                DataBuffer cbuf = out.bufferRef();
                LOGGER.trace("{}  parseBlob  write len2 {}  at cbuf {}  ts {} {}", reqctx, 17 + valueBytes, cbuf.writePosition(), ts / 1000000000, ts % 1000000000);
                ByteBuffer bbuf = cbuf.asByteBuffer(cbuf.writePosition(), 4);
                cbuf.writePosition(cbuf.writePosition() + 4);
                bbuf.putInt(17 + valueBytes);
            }
            state = State.EXPECT_SECOND_LENGTH;
            needMin = Integer.BYTES;
        }
    }

    void parseSecondLength(DataBuffer buf) {
        int pr = buf.readPosition();
        int pw = buf.writePosition();
        ByteBuffer bb = buf.asByteBuffer(buf.readPosition(), needMin);
        buf.readPosition(buf.readPosition() + needMin);
        int len2 = bb.getInt();
        if (len2 != -1 && len2 != 0 && len2 != blobLength) {
            LOGGER.error("{}  event blob length mismatch  {}  seenHeaderA {}  at {}  len2 {}  blobLength {}  pulse {}", reqctx, channelName, seenHeaderA, buf.readPosition(), len2, blobLength, pulse);
            throw new RuntimeException("unexpected 2nd length");
        }
        state = State.EXPECT_HEADER_A;
        needMin = HEADER_A_LEN;
        writtenEvents += 1;
        if (qp.limitEventsPerChannel > 0 && writtenEvents >= qp.limitEventsPerChannel) {
            LOGGER.warn("{}  limit reached  writtenBytes {}  writtenEvents {}  limit {}  channel {}", reqctx, writtenBytes, writtenEvents, qp.limitEventsPerChannel, channelName);
            state = State.TERM;
        }
        //LOGGER.info("parseSecondLength  ts {}  valueBytes {}", ts, valueBytes);
    }

    public synchronized void release() {
        LOGGER.debug("{}  EventBlobToV1Map release  seenHeaderA {}  writtenBytes {}", reqctx, seenHeaderA, writtenBytes);
        if (leftbufcont != null) {
            BufCont k = leftbufcont;
            leftbufcont = null;
            k.close();
        }
        if (kbufcont != null) {
            BufCont k = kbufcont;
            kbufcont = null;
            k.close();
        }
    }

    public void releaseFinite() {
        release();
    }

    public synchronized EventBlobMapResult lastResult() {
        LOGGER.debug("until now total written {}", writtenBytes);
        if (!headerOut) {
            LOGGER.debug("{}  EventBlobToV1Map lastResult  not headerOut", reqctx);
            Output out = new Output(bufFac, bufferSize2, BufCont.Mark.V1MapLast);
            writeHeader(out, channelName);
            return EventBlobMapResult.fromBuffers(out.take());
        }
        else {
            return EventBlobMapResult.empty();
        }
    }

    void writeHeader(Output out, String channelName) {
        BlobJsonHeader header = new BlobJsonHeader();
        header.name = channelName;
        if (blobsType != -1) {
            header.type = DTypeBitmapUtils.Type.lookup((short) blobsType).toString().toLowerCase();
            if (blobsCompression == 0 || blobUnpack) {
                header.compression = "0";
            }
            else if (blobsCompression == 1) {
                // bitshuffle lz4
                header.compression = "1";
            }
            else if (blobsCompression == 2) {
                // lz4
                header.compression = "2";
            }
            else {
                throw new RuntimeException("logic");
            }
            header.byteOrder = blobsByteOrder.toString();
            header.array = blobsArray == 1;
            if (shapeDimsHeader > 0) {
                header.shape = new ArrayList<>();
                for (int i = 0; i < shapeDimsHeader; i++) {
                    header.shape.add(shapeLensHeader[i]);
                }
            }
        }
        String headerString;
        try {
            headerString = new ObjectMapper(new JsonFactory()).writeValueAsString(header);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException("JsonProcessingException");
        }
        LOGGER.trace("{}  headerString {}", reqctx, headerString);
        ByteBuffer headerEncoded = StandardCharsets.UTF_8.encode(headerString);
        int nh = headerEncoded.remaining();
        int n = Integer.BYTES + Byte.BYTES + nh + Integer.BYTES;
        {
            out.ensureWritable(n);
            DataBuffer cbuf = out.bufferRef();
            ByteBuffer bb = cbuf.asByteBuffer(cbuf.writePosition(), n);
            cbuf.writePosition(cbuf.writePosition() + n);
            bb.putInt(0);
            bb.put((byte) 0);
            bb.put(headerEncoded);
            if (bb.position() != 5 + nh) {
                throw new RuntimeException("logic");
            }
            bb.putInt(0, 1 + nh);
            bb.putInt(1 + nh);
        }
    }

}
