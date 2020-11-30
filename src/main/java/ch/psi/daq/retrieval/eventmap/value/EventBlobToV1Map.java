package ch.psi.daq.retrieval.eventmap.value;

import ch.psi.bitshuffle.BitShuffleLZ4JNIDecompressor;
import ch.psi.daq.retrieval.DTypeBitmapUtils;
import ch.psi.daq.retrieval.ReqCtx;
import ch.qos.logback.classic.Level;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.jpountz.lz4.LZ4Factory;
import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DataBufferUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class EventBlobToV1Map implements Function<DataBuffer, EventBlobMapResult> {
    static final Logger LOGGER = (Logger) LoggerFactory.getLogger("EventBlobToV1Map");
    static final int HEADER_A_LEN = 2 * Integer.BYTES + 4 * Long.BYTES + 2 * Byte.BYTES;
    DataBufferFactory bufFac;
    DataBuffer cbuf;
    DataBuffer kbuf;
    DataBuffer left;
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
    int shapeDims;
    int missingBytes;
    int valueBytes;
    int[] shapeLens = new int[4];
    ByteOrder blobsByteOrder = null;
    long pulse;
    long ts;
    int blobLength;
    int headerLength;
    int optionalFieldsLength;
    long endNanos;
    boolean unpackOnServer;
    boolean blobUnpack;
    long limitBytes;
    long writtenBytes;
    long seenHeaderA;
    ReqCtx reqctx;

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

    public EventBlobToV1Map(ReqCtx reqctx, String channelName, long endNanos, DataBufferFactory bufFac, int bufferSize, boolean unpackOnServer, long limitBytes) {
        this.reqctx = reqctx;
        this.channelName = channelName;
        this.bufFac = bufFac;
        this.bufferSize = bufferSize;
        this.bufferSize2 = 2 * bufferSize;
        this.endNanos = endNanos;
        this.unpackOnServer = unpackOnServer;
        this.state = State.EXPECT_HEADER_A;
        this.needMin = HEADER_A_LEN;
        this.limitBytes = limitBytes;
    }

    public static Flux<EventBlobMapResult> trans2(ReqCtx reqctx, Flux<DataBuffer> fl, String channelName, long endNanos, DataBufferFactory bufFac, int bufferSize, boolean unpackOnServer, long limitBytes) {
        final EventBlobToV1Map mapper = new EventBlobToV1Map(reqctx, channelName, endNanos, bufFac, bufferSize, unpackOnServer, limitBytes);
        return fl.map(mapper)
        .concatWith(Mono.defer(() -> Mono.just(mapper.lastResult())))
        .doOnNext(item -> {
            if (item.term) {
                LOGGER.warn("{}  EventBlobToV1Map reached TERM", reqctx);
            }
        })
        .takeWhile(item -> !item.term)
        .doOnDiscard(EventBlobMapResult.class, EventBlobMapResult::release)
        .doOnTerminate(mapper::release);
    }

    @Override
    public EventBlobMapResult apply(DataBuffer buf) {
        LOGGER.trace("{}  EventBlobToV1Map  apply  state: {}  buf: {}", reqctx, state, buf);
        if (state == State.TERM) {
            LOGGER.debug("{}  apply buffer despite TERM", reqctx);
            DataBufferUtils.release(buf);
            EventBlobMapResult res = new EventBlobMapResult();
            res.term = true;
            return res;
        }
        cbuf = bufFac.allocateBuffer(bufferSize2);
        if (left != null) {
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
                LOGGER.debug("{}  parse left  {}", reqctx, state);
                parse(left);
                if (left.readableByteCount() != 0) {
                    throw new RuntimeException("logic");
                }
                DataBufferUtils.release(left);
                left = null;
            }
        }

        while (state != State.TERM && buf.readableByteCount() > 0 && buf.readableByteCount() >= needMin) {
            LOGGER.debug("{}  parse main  {}  {}", reqctx, state, buf.readPosition());
            parse(buf);
        }

        if (state != State.TERM && buf.readableByteCount() > 0) {
            if (needMin <= 0) {
                throw new RuntimeException("logic");
            }
            if (buf.readableByteCount() >= needMin) {
                throw new RuntimeException("logic");
            }
            LOGGER.debug("{}  keep left", reqctx);
            left = buf;
        }
        else {
            DataBufferUtils.release(buf);
        }
        DataBuffer rbuf = cbuf;
        cbuf = null;
        EventBlobMapResult res = new EventBlobMapResult();
        res.buf = rbuf;
        writtenBytes += res.buf.readableByteCount();
        return res;
    }

    void parse(DataBuffer buf) {
        LOGGER.debug("{}  parse  state: {}  buf: {}", reqctx, state, buf);
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
        if (ts < 100100100100100100L || ts > 1800100100100100100L) {
            LOGGER.warn("{}  unexpected ts {}", reqctx, ts);
            //throw new RuntimeException("error");
        }
        if (pulse < -1 || pulse > 40100100100L) {
            LOGGER.warn("{}  unexpected pulse {}", reqctx, pulse);
            //throw new RuntimeException("error");
        }
        if (ts >= endNanos) {
            LOGGER.debug("{}  stop  ts {}  >=  end {}", reqctx, ts, endNanos);
            state = State.TERM;
            return;
        }
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
        int dtypeBitmask = 0xffff & bb.getShort();
        blobsType = 0xff & dtypeBitmask;
        if (blobsType > 13) {
            LOGGER.error("{}  unexpected datatype: {}", reqctx, blobsType);
            throw new RuntimeException("unexpected datatype");
        }
        if ((dtypeBitmask & 0x8000) != 0) {
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
        state = State.EXPECT_HEADER_C;
        //LOGGER.info("{}  end of header C  type {}  array {}", reqctx, blobsType, blobsArray);
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
            shapeDims = 0xff & bb.get();
            needMin = shapeDims * Integer.BYTES;
            LOGGER.debug("{}  has shape, dims: {}", reqctx, shapeDims);
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
        for (int i = 0; i < shapeDims; i++) {
            int n = bb.getInt();
            shapeLens[i] = n;
        }
        cbuf = ensureWritable(cbuf, 256 + valueBytes);
        if (blobUnpack) {
            if (true) {
                LOGGER.error("{}  blobUnpack not supported currently", reqctx);
                throw new RuntimeException("todo");
            }

            if (kbuf == null) {
                kbuf = bufFac.allocateBuffer(bufferSize2);
            }
        }
        state = State.EXPECT_BLOBS;
        needMin = 0;
        // ugly fixes for bug in writer
        if (blobsCompression > 0) {
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

    DataBuffer ensureWritable(DataBuffer buf, int n) {
        int now = buf.writableByteCount();
        if (now >= n) {
            return buf;
        }
        int wp = buf.writePosition();
        while (bufferSize2 < wp + n) {
            bufferSize2 *= 2;
            if (bufferSize2 > 32 * 1024 * 1024) {
                throw new RuntimeException("maximum buffer size exceeded");
            }
        }
        buf.ensureCapacity(bufferSize2);
        return buf;
    }

    void parseBlob(DataBuffer buf) {
        final int n = Math.min(buf.readableByteCount(), missingBytes);
        // TODO do we enforce same compression for all blobs?
        if (blobUnpack) {
            if (true) {
                LOGGER.error("{}  blobUnpack not supported currently", reqctx);
                throw new RuntimeException("todo");
            }
            kbuf = ensureWritable(kbuf, n);
            kbuf.write(buf.slice(buf.readPosition(), n));
            buf.readPosition(buf.readPosition() + n);
        }
        else {
            ensureWritable(cbuf, 21 + n);
            if (!blobHeaderOut) {
                // ugly fixes for bug in writer
                if (blobsArray == 1 && blobsShape != 1) {
                    if (buf.readableByteCount() < 8) {
                        throw new RuntimeException("logic");
                    }
                    ByteBuffer src = buf.asByteBuffer(buf.readPosition(), 8);
                    int nf = (int) src.getLong();
                    LOGGER.trace("{}  current shape: {}  {}", reqctx, shapeDims, shapeLens);
                    shapeDims = 1;
                    shapeLens[0] = nf / sizeOf(blobsType);
                }
                if (!headerOut) {
                    writeHeader(cbuf, channelName);
                    headerOut = true;
                }
                LOGGER.trace("{}  write len1 {}  at {}", reqctx, 17 + valueBytes, cbuf.writePosition());
                ByteBuffer bbuf = cbuf.asByteBuffer(cbuf.writePosition(), 21);
                cbuf.writePosition(cbuf.writePosition() + 21);
                bbuf.putInt(17 + valueBytes);
                bbuf.put((byte) 1);
                bbuf.putLong(ts);
                bbuf.putLong(pulse);
                blobHeaderOut = true;
            }
            cbuf.write(buf.slice(buf.readPosition(), n));
            buf.readPosition(buf.readPosition() + n);
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
                        writeHeader(cbuf, channelName);
                        headerOut = true;
                    }
                    int m = nf;
                    cbuf = ensureWritable(cbuf, m + 128);
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
                LOGGER.trace("{}  parseBlob  write len2 {}  at cbuf {}  ts {} {}", reqctx, 17 + valueBytes, cbuf.writePosition(), ts / 1000000000, ts % 1000000000);
                cbuf = ensureWritable(cbuf, 4);
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
        if (limitBytes > 0 && writtenBytes >= limitBytes) {
            LOGGER.warn("{}  limit reached.  {}  written {}  limit {}", reqctx, channelName, writtenBytes, limitBytes);
            state = State.TERM;
        }
    }

    public void release() {
        LOGGER.debug("{}  EventBlobToV1Map release  seenHeaderA {}  writtenBytes {}", reqctx, seenHeaderA, writtenBytes);
        if (cbuf != null) {
            DataBufferUtils.release(cbuf);
            cbuf = null;
        }
        if (left != null) {
            DataBufferUtils.release(left);
            left = null;
        }
        if (kbuf != null) {
            DataBufferUtils.release(kbuf);
            kbuf = null;
        }
    }

    public EventBlobMapResult lastResult() {
        LOGGER.debug("{}  EventBlobToV1Map lastResult", reqctx);
        DataBuffer buf = bufFac.allocateBuffer(bufferSize2);
        if (!headerOut) {
            writeHeader(buf, channelName);
        }
        EventBlobMapResult res = new EventBlobMapResult();
        res.buf = buf;
        return res;
    }

    void writeHeader(DataBuffer buf, String channelName) {
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
            if (shapeDims > 0) {
                header.shape = new ArrayList<>();
                for (int i = 0; i < shapeDims; i++) {
                    header.shape.add(shapeLens[i]);
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
        ByteBuffer bb1 = buf.asByteBuffer(buf.writePosition(), n);
        buf.writePosition(buf.writePosition() + n);
        bb1.putInt(0);
        bb1.put((byte)0);
        bb1.put(headerEncoded);
        if (bb1.position() != 5 + nh) {
            throw new RuntimeException("logic");
        }
        bb1.putInt(0, 1 + nh);
        bb1.putInt(1 + nh);
    }

}
