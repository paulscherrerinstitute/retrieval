package ch.psi.daq.retrieval.eventmap.value;

import ch.psi.bitshuffle.BitShuffleLZ4JNIDecompressor;
import ch.psi.daq.retrieval.DTypeBitmapUtils;
import ch.psi.daq.retrieval.EventDataType;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import ch.qos.logback.classic.Logger;
import org.reactivestreams.Publisher;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DataBufferUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static ch.psi.daq.retrieval.EventDataType.*;

public class EventBlobToJsonMap implements Function<DataBuffer, MapJsonResult> {
    static Logger LOGGER = (Logger) LoggerFactory.getLogger("EventBlobToJsonMap");
    static {
        //LOGGER.setLevel(Level.INFO);
    }
    static final int HEADER_A_LEN = 2 * Integer.BYTES + 4 * Long.BYTES + 2 * Byte.BYTES;
    DataBufferFactory bufFac;
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
    long limitBytes;
    long seenHeaderA;
    boolean doTrace = false;

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

    public EventBlobToJsonMap(String channelName, long endNanos, DataBufferFactory bufFac, int bufferSize, long limitBytes) {
        this.channelName = channelName;
        this.bufFac = bufFac;
        this.bufferSize = bufferSize;
        this.bufferSize2 = 2 * bufferSize;
        this.endNanos = endNanos;
        this.state = State.EXPECT_HEADER_A;
        this.needMin = HEADER_A_LEN;
        this.limitBytes = limitBytes;
    }

    public static Flux<MapJsonResult> trans2(Flux<DataBuffer> fl, String channelName, long endNanos, DataBufferFactory bufFac, int bufferSize, long limitBytes) {
        EventBlobToJsonMap mapper = new EventBlobToJsonMap(channelName, endNanos, bufFac, bufferSize, limitBytes);
        return fl.map(mapper)
        .concatWith(Mono.defer(() -> Mono.just(mapper.lastResult())))
        .doOnNext(item -> {
            if (item.term) {
                LOGGER.warn("reached TERM");
            }
        })
        .takeWhile(item -> !item.term)
        .doOnDiscard(MapJsonResult.class, MapJsonResult::release)
        .doOnTerminate(mapper::release);
    }

    public static Function<Flux<DataBuffer>, Publisher<MapJsonResult>> trans(String channelName, long endNanos, DataBufferFactory bufFac, int bufferSize, long limitBytes) {
        EventBlobToJsonMap mapper = new EventBlobToJsonMap(channelName, endNanos, bufFac, bufferSize, limitBytes);
        return fl -> {
            return fl.map(mapper)
            .concatWith(Mono.defer(() -> Mono.just(mapper.lastResult())))
            .doOnNext(item -> {
                if (item.term) {
                    LOGGER.warn("reached TERM");
                }
            })
            .takeWhile(item -> !item.term)
            .doOnDiscard(MapJsonResult.class, MapJsonResult::release)
            .doOnTerminate(mapper::release);
        };
    }

    @Override
    public MapJsonResult apply(DataBuffer buf) {
        LOGGER.trace("apply  state: {}  buf: {}", state, buf);
        MapJsonResult res = new MapJsonResult();
        if (state == State.TERM) {
            //LOGGER.info("apply buffer despite TERM");
            DataBufferUtils.release(buf);
            res.term = true;
            return res;
        }
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
                LOGGER.debug("parse left  {}", state);
                parse(res, left);
                if (left.readableByteCount() != 0) {
                    throw new RuntimeException("logic");
                }
                DataBufferUtils.release(left);
                left = null;
            }
        }

        while (state != State.TERM && buf.readableByteCount() > 0 && buf.readableByteCount() >= needMin) {
            LOGGER.debug("parse main  {}  {}", state, buf.readPosition());
            parse(res, buf);
        }

        if (state != State.TERM && buf.readableByteCount() > 0) {
            if (needMin <= 0) {
                throw new RuntimeException("logic");
            }
            if (buf.readableByteCount() >= needMin) {
                throw new RuntimeException("logic");
            }
            LOGGER.debug("keep left");
            left = buf;
        }
        else {
            DataBufferUtils.release(buf);
        }
        //LOGGER.error("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~  {}", res.items.size());
        return res;
    }

    void parse(MapJsonResult res, DataBuffer buf) {
        LOGGER.debug("parse  state: {}  buf: {}", state, buf);
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
            parseBlob(res, buf);
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
            LOGGER.info("Stop because length == 0");
            state = State.TERM;
            return;
        }

        if (length < 0 || length > 60 * 1024 * 1024) {
            LOGGER.error(String.format("Stop because unexpected length: %d", length));
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
        LOGGER.trace("seen  length {}  timestamp {}  pulse {}  seenHeaderA {}", length, ts, pulse, seenHeaderA);
        if (ts < 1200100100100100100L || ts > 1700100100100100100L) {
            LOGGER.error("unexpected ts {}", ts);
            throw new RuntimeException("error");
        }
        if (pulse < 0 || pulse > 20100100100L) {
            LOGGER.debug("unexpected pulse {}", pulse);
            //throw new RuntimeException("error");
        }
        if (ts >= endNanos) {
            LOGGER.info("stop  ts {}  >=  end {}", ts, endNanos);
            state = State.TERM;
            return;
        }
        int optionalFieldsLength = bb.getInt();
        if (optionalFieldsLength < -1 || optionalFieldsLength == 0) {
            LOGGER.error("unexpected value for optionalFieldsLength: {}", optionalFieldsLength);
            throw new RuntimeException("unexpected optional fields");
        }
        if (optionalFieldsLength != -1) {
            LOGGER.warn("Found optional fields: {}", optionalFieldsLength);
        }
        if (optionalFieldsLength > 2048) {
            LOGGER.error("unexpected optional fields: {}", optionalFieldsLength);
            throw new RuntimeException("unexpected optional fields");
        }
        headerLength = HEADER_A_LEN;
        this.pulse = pulse;
        this.ts = ts;
        this.optionalFieldsLength = Math.max(optionalFieldsLength, 0);
        this.blobLength = length;
        if (blobLength < 1 || blobLength > 20 * 1024 * 1024) {
            LOGGER.error("Unexpected blobLength {}", blobLength);
            throw new RuntimeException("Unexpected blobLength");
        }
        state = State.EXPECT_HEADER_B;
        needMin = this.optionalFieldsLength + Short.BYTES;
    }

    void parseHeaderB(DataBuffer buf) {
        ByteBuffer bb = buf.asByteBuffer(buf.readPosition(), needMin);
        buf.readPosition(buf.readPosition() + needMin);
        if (bb.position() != 0) {
            LOGGER.warn("readHeaderB test position {}", bb.position());
        }
        headerLength += needMin;
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
        state = State.EXPECT_HEADER_C;
        //LOGGER.info(String.format("end of header C:  type: %d  array: %d", blobsType, blobsArray));
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
        LOGGER.trace("blobLength {}  headerLength {}  missing {}", blobLength, headerLength, valueBytes);
        for (int i = 0; i < shapeDims; i++) {
            int n = bb.getInt();
            shapeLens[i] = n;
        }
        state = State.EXPECT_BLOBS;
        needMin = 0;
        // ugly fixes for bug in writer
        if (blobsCompression > 0) {
            needMin = 8;
        }
    }

    DataBuffer ensureWritable(DataBuffer buf, int n) {
        if (buf == null) {
            buf = bufFac.allocateBuffer(bufferSize2);
        }
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
        DataBuffer old = buf;
        int rp0 = old.readPosition();
        int wp0 = old.writePosition();
        buf = bufFac.allocateBuffer(bufferSize2);
        buf.write(old.slice(0, wp0));
        buf.writePosition(wp0);
        buf.readPosition(rp0);
        return buf;
    }

    void writeEvent(MapJsonResult res) {
        EventDataType edt = EventDataType.vals[blobsType];
        if (blobsCompression == 0) {
            //LOGGER.info("WRITE UNCOMPRESSED");
            if (kbuf.readableByteCount() != edt.sizeOf()) {
                throw new RuntimeException("error");
            }
            ByteBuffer src = kbuf.asByteBuffer(0, kbuf.writePosition());
            if (blobsArray == 0) {
                //LOGGER.info("WRITE SCALAR  len {}", kbuf.readableByteCount());
                if (!headerOut) {
                    writeHeader(res, channelName);
                }
                MapJsonEvent ev = new MapJsonEvent();
                ev.ts = ts;
                ev.pulse = pulse;
                res.items.add(ev);
                if (edt == UINT8) {
                    ev.data = JsonNodeFactory.instance.numberNode(0xff & src.get());
                }
                else if (edt == UINT16) {
                    ev.data = JsonNodeFactory.instance.numberNode(0xff & src.getShort());
                }
                else if (edt == UINT32) {
                    ev.data = JsonNodeFactory.instance.numberNode(0xffL & src.getInt());
                }
                else if (edt == UINT64) {
                    ev.data = JsonNodeFactory.instance.numberNode(src.getLong());
                }
                else if (edt == INT8) {
                    ev.data = JsonNodeFactory.instance.numberNode(src.get());
                }
                else if (edt == INT16) {
                    ev.data = JsonNodeFactory.instance.numberNode(src.getShort());
                }
                else if (edt == INT32) {
                    ev.data = JsonNodeFactory.instance.numberNode(src.getInt());
                }
                else if (edt == INT64) {
                    ev.data = JsonNodeFactory.instance.numberNode(src.getLong());
                }
                else if (edt == FLOAT32) {
                    float v = src.getFloat();
                    ev.data = JsonNodeFactory.instance.numberNode(v);
                }
                else if (edt == FLOAT64) {
                    double v = src.getDouble();
                    ev.data = JsonNodeFactory.instance.numberNode(v);
                }
                else {
                    throw new RuntimeException("todo");
                }
            }
            else {
                LOGGER.error("++++++  TODO need test data  ++++++");
                throw new RuntimeException("todo");
            }
        }
        else if (blobsCompression == 1) {
            ByteBuffer src = kbuf.asByteBuffer(0, kbuf.writePosition());
            int nbytes = (int) src.getLong();
            if (nbytes > 1024 * 128) {
                LOGGER.info("large compressed waveform  channel {}  pulse {}  nbytes {}", channelName, pulse, nbytes);
                throw new RuntimeException("large waveform");
            }
            int tsize = edt.sizeOf();
            int nf = nbytes / tsize;
            ByteBuffer dst = ByteBuffer.allocate((20 * kbuf.readableByteCount() / 1024 + 1) * 1024);
            BitShuffleLZ4JNIDecompressor decompressor = new BitShuffleLZ4JNIDecompressor();
            int bs = src.getInt();
            LOGGER.trace("pulse: {}  nf: {}  bs: {}  ty: {}  order: {}", pulse, nf, bs, blobsType, blobsByteOrder);
            int a = decompressor.decompress(src, 12, dst, 0, nf, tsize, bs);
            dst.limit(nbytes);
            dst.position(0);
            if (!headerOut) {
                // ugly fixes for bug in writer
                if (blobsArray == 1 && blobsShape != 1) {
                    //LOGGER.info("current shape: {}  {}", shapeDims, shapeLens);
                    shapeDims = 1;
                    shapeLens[0] = nf;
                }
                // TODO can I have a single call-site to writeHeader in this class?
                writeHeader(res, channelName);
            }
            MapJsonEvent ev = new MapJsonEvent();
            ev.ts = ts;
            ev.pulse = pulse;
            res.items.add(ev);
            if (edt.isInt()) {
                if (edt.isSignedInt()) {
                    if (blobsArray == 1) {
                        // TODO should the json value already be shaped? matter only for 1d, images are usually not fetched as json
                        ArrayNode node = JsonNodeFactory.instance.arrayNode(nf);
                        while (dst.remaining() > 0) {
                            if (tsize == 1) {
                                byte v = dst.get();
                                node.add(v);
                            }
                            else if (tsize == 2) {
                                short v = dst.getShort();
                                node.add(v);
                            }
                            else if (tsize == 4) {
                                int v = dst.getInt();
                                node.add(v);
                            }
                            else if (tsize == 8) {
                                long v = dst.getLong();
                                node.add(v);
                            }
                            else {
                                throw new RuntimeException("todo");
                            }
                        }
                        ev.data = node;
                    }
                    else {
                        LOGGER.error("blobsArray {}", blobsArray);
                        throw new RuntimeException("todo");
                    }
                }
                else if (edt.isUnsignedInt()) {
                    if (blobsArray == 1) {
                        // TODO should the json value already be shaped? matter only for 1d, images are usually not fetched as json
                        ArrayNode node = new ArrayNode(JsonNodeFactory.instance);
                        while (dst.remaining() > 0) {
                            if (tsize == 1) {
                                int v = 0xff & dst.get();
                                node.add(v);
                            }
                            else if (tsize == 2) {
                                int v = 0xff & dst.getShort();
                                node.add(v);
                            }
                            else if (tsize == 4) {
                                long v = 0xffL & dst.getInt();
                                node.add(v);
                            }
                            else if (tsize == 8) {
                                long v = dst.getLong();
                                if (v < 0) {
                                    throw new RuntimeException("Java has no unsigned 64 bit integer");
                                }
                                node.add(v);
                            }
                            else {
                                throw new RuntimeException("todo");
                            }
                        }
                        ev.data = node;
                    }
                    else {
                        LOGGER.error("blobsArray {}", blobsArray);
                        throw new RuntimeException("todo");
                    }
                }
                else {
                    throw new RuntimeException("todo");
                }
            }
            else {
                throw new RuntimeException("todo");
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

    void parseBlob(MapJsonResult res, DataBuffer buf) {
        final int n = Math.min(buf.readableByteCount(), missingBytes);
        // TODO do we enforce same compression for all blobs?
        kbuf = ensureWritable(kbuf, n);
        kbuf.write(buf.slice(buf.readPosition(), n));
        buf.readPosition(buf.readPosition() + n);
        missingBytes -= n;
        if (missingBytes > 0) {
            needMin = 0;
        }
        else {
            writeEvent(res);
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
        // TODO attention debug code!
        if (len2 != -1 && len2 != 0 && len2 != blobLength) {
            LOGGER.error("event blob length mismatch  seenHeaderA {}  at {}  len2 {}  blobLength {}  pulse {}", seenHeaderA, buf.readPosition(), len2, blobLength, pulse);
            int n1 = Math.max(0, pr - 16);
            int n2 = Math.min(pw, pr + 16);
            StringBuffer sbuf = new StringBuffer();
            for (int i1 = n1; i1 < n2; i1 += 1) {
                sbuf.append(String.format("%02x ", buf.getByte(i1)));
            }
            LOGGER.error("Context: {}", sbuf);
            throw new RuntimeException("unexpected 2nd length");
        }
        state = State.EXPECT_HEADER_A;
        needMin = HEADER_A_LEN;
    }

    public void release() {
        LOGGER.info("release  seenHeaderA {}", seenHeaderA);
        if (left != null) {
            DataBufferUtils.release(left);
            left = null;
        }
        if (kbuf != null) {
            DataBufferUtils.release(kbuf);
            kbuf = null;
        }
    }

    public MapJsonResult lastResult() {
        LOGGER.info("lastResult");
        // TODO can I ever have a last result?
        // TODO is the default state a valid empty state?
        MapJsonResult res = new MapJsonResult();
        if (!headerOut) {
            writeHeader(res, channelName);
        }
        return res;
    }

    void writeHeader(MapJsonResult res, String channelName) {
        MapJsonChannelStart mapev = new MapJsonChannelStart();
        mapev.name = channelName;
        res.items.add(mapev);
        BlobJsonHeader header = new BlobJsonHeader();
        header.name = channelName;
        if (blobsType != -1) {
            mapev.type = DTypeBitmapUtils.Type.lookup((short) blobsType).toString().toLowerCase();
            mapev.byteOrder = blobsByteOrder;
            mapev.array = blobsArray == 1;
            mapev.shape = Arrays.stream(shapeLens, 0, shapeDims).boxed().collect(Collectors.toList());
        }
        headerOut = true;
    }

}
