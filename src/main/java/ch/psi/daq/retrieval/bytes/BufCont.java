package ch.psi.daq.retrieval.bytes;

import ch.psi.daq.retrieval.error.ErrorUtils;
import ch.psi.daq.retrieval.merger.Releasable;
import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import reactor.core.publisher.Mono;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.ref.Cleaner;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

public class BufCont implements AutoCloseable, Releasable {
    static final Logger LOGGER = (Logger) LoggerFactory.getLogger(BufCont.class.getSimpleName());
    static final AtomicLong openedCount = new AtomicLong();
    static final AtomicLong openedBytes = new AtomicLong();
    static final AtomicLong closedCount = new AtomicLong();
    static final AtomicLong closedBytes = new AtomicLong();
    static final AtomicLong openedClosedCount = new AtomicLong();
    static final AtomicLong openedClosedBytes = new AtomicLong();
    static final AtomicLong cleanerRunCount = new AtomicLong();
    static final AtomicLong cleanerRunNomarkCount = new AtomicLong();
    static final AtomicLong cleanerRunBytes = new AtomicLong();
    static final AtomicLong cleanerRunNomarkBytes = new AtomicLong();
    static final AtomicLong errorCount = new AtomicLong();
    static final AtomicLong closeErrorCount = new AtomicLong();
    static final AtomicLong closeCapMismatchCount = new AtomicLong();
    static final AtomicLong closeCapMismatchBytes = new AtomicLong();
    static final AtomicLong againClosedCount = new AtomicLong();
    static final Cleaner cleaner = Cleaner.create();
    static final AtomicLong idCount = new AtomicLong();
    static final AtomicLong gcCount = new AtomicLong();
    static final AtomicLong takeButClosed = new AtomicLong();
    static final AtomicLong takeButEmpty = new AtomicLong();
    static final AtomicLong takeClosedNotEmpty = new AtomicLong();

    static class Inner implements Runnable {
        DataBuffer buf;
        boolean mark;
        Inner() {}
        @Override
        public synchronized void run() {
            cleanerRunCount.getAndAdd(1);
            if (!mark) {
                cleanerRunNomarkCount.getAndAdd(1);
            }
            if (buf != null) {
                DataBuffer buf2 = buf;
                buf = null;
                int cap = buf2.capacity();
                DataBufferUtils.release(buf2);
                cleanerRunBytes.getAndAdd(cap);
                if (!mark) {
                    cleanerRunNomarkBytes.getAndAdd(cap);
                }
            }
        }
    }

    public static class Stats {
        public long errorCount;
        public long againClosedCount;
        public long openedCount;
        public long openedBytes;
        public long closedCount;
        public long closedBytes;
        public long openedClosedCount;
        public long openedClosedBytes;
        public long cleanerRunCount;
        public long cleanerRunBytes;
        public long cleanerRunNomarkCount;
        public long cleanerRunNomarkBytes;
        public long allEverLen;
        public long closeErrorCount;
        public long closeCapMismatchCount;
        public long closeCapMismatchBytes;
        public long gcCount;
        public long takeButClosed;
        public long takeButEmpty;
        public long takeClosedNotEmpty;
        public Stats() {
            errorCount = BufCont.errorCount.get();
            againClosedCount = BufCont.againClosedCount.get();
            openedCount = BufCont.openedCount.get();
            openedBytes = BufCont.openedBytes.get();
            closedCount = BufCont.closedCount.get();
            closedBytes = BufCont.closedBytes.get();
            openedClosedCount = BufCont.openedClosedCount.get();
            openedClosedBytes = BufCont.openedClosedBytes.get();
            cleanerRunCount = BufCont.cleanerRunCount.get();
            cleanerRunBytes = BufCont.cleanerRunBytes.get();
            cleanerRunNomarkCount = BufCont.cleanerRunNomarkCount.get();
            cleanerRunNomarkBytes = BufCont.cleanerRunNomarkBytes.get();
            synchronized (allEver) {
                allEverLen = BufCont.allEver.size();
            }
            closeErrorCount = BufCont.closeErrorCount.get();
            closeCapMismatchCount = BufCont.closeCapMismatchCount.get();
            closeCapMismatchBytes = BufCont.closeCapMismatchBytes.get();
            gcCount = BufCont.gcCount.get();
            takeButClosed = BufCont.takeButClosed.get();
            takeButEmpty = BufCont.takeButEmpty.get();
            takeClosedNotEmpty = BufCont.takeClosedNotEmpty.get();
        }
    }

    public enum Mark {
        __NOMARK,
        ALLOCATE,
        CLONED_FROM,
        CLONED_EMPTY,
        Flatten,
        FlattenPassOn,
        msiv1,
        ITEM_VEC_VGEN,
        ITEM_VEC_TFL,
        ITEM_VEC_IB_CTOR,
        MtivIFlA,
        MtivTB1,
        MtivTB2,
        SUB_IN_CAN_REL,
        SUB_IN,
        SUB_OUT_NEXT,
        qdrl1,
        qdrl2,
        qdrl3,
        qdrl4,
        qdrl7,
        MERGER_FAKE,
        BuildMergedAfterSub,
        QUERY_DATA_MERGED_01,
        QUERY_DATA_MERGED_02,
        TransMapQueryMerged_01,
        TransMapQueryMerged_02,
        MapTs_trans2,
        MapTs_apply,
        MapTs_keep_left,
        OutNew,
        OutAdd,
        V1Map,
        V1MapLast,
        V1Map_init,
        V1Map_left_alloc,
        V1Map_kbuf_alloc,
        MapResultTaken,
        ChEvS_buf,
        JsMap_init,
        JsMap_init_2nd,
        TestA,
        MapBasicBufBegin,
        PreChanConf,
        SprWebCl,
        SprTest,
        RawCl1,
        RawCl2,
        RawSub1,
        RawSub2,
        SubTools1,
        SubTools2,
        SubTools3,
        SubToolsBuf,
        Parts1,
        Parts2,
        PartsNextDIS,
        mergeItemVecFluxesA,
        mergeItemVecFluxesB,
        OcLo1,
        OcLo2,
        OcLo3,
        Acc1,
        Acc2,
    }

    public static final boolean doMark = true;
    static final boolean storeTraceAccess = false;
    static final boolean addToAll = true;
    static final boolean removeFromAllOnClose = true;
    static final ConcurrentHashMap<Long, BufCont> allEver = new ConcurrentHashMap<>();
    static final int allEverMax = 300;
    static AtomicLongArray markHist;
    static {
        initMarkHist();
    }
    long id;
    int cap;
    Cleaner.Cleanable cleanable;
    Inner inner;
    byte closed;
    byte taken;
    boolean isCloned;
    final Mark[] marks = new Mark[32];
    int markIx;
    int markCount;
    Throwable eCtor;
    Throwable eClosed;
    Mono<Integer> emClosed;
    List<Throwable> refAccesses;
    long ts = System.nanoTime();

    public static BufCont allocate(DataBufferFactory bufFac, int len, Mark mark) {
        BufCont ret = new BufCont(mark);
        try {
            Inner inner = new Inner();
            ret.cleanable = cleaner.register(ret, inner);
            ret.inner = inner;
            ret.inner.buf = bufFac.allocateBuffer(len);
            int cap = ret.inner.buf.capacity();
            ret.cap = cap;
            openedCount.getAndAdd(1);
            openedBytes.getAndAdd(cap);
            openedClosedCount.getAndAdd(1);
            openedClosedBytes.getAndAdd(cap);
            return ret;
        }
        catch (OutOfMemoryError e) {
            errorCount.getAndAdd(1);
            ByteArrayOutputStream ba = new ByteArrayOutputStream();
            PrintStream ps = new PrintStream(ba);
            new Exception().printStackTrace(ps);
            ps.close();
            LOGGER.error("BufCont OutOfMemoryError\n{}", ba.toString(StandardCharsets.UTF_8));
            ret.close();
            throw e;
        }
    }

    public static BufCont fromBuffer(DataBuffer buf, Mark mark) {
        BufCont ret = new BufCont(mark);
        try {
            Inner inner = new Inner();
            ret.cleanable = cleaner.register(ret, inner);
            ret.inner = inner;
            ret.inner.buf = buf;
            int cap = ret.inner.buf.capacity();
            ret.cap = cap;
            openedCount.getAndAdd(1);
            openedBytes.getAndAdd(cap);
            openedClosedCount.getAndAdd(1);
            openedClosedBytes.getAndAdd(cap);
            return ret;
        }
        catch (OutOfMemoryError e) {
            errorCount.getAndAdd(1);
            LOGGER.warn("BufCont fromBuffer OutOfMemoryError");
            ret.close();
            throw e;
        }
    }

    public static BufCont makeEmpty(Mark mark) {
        openedCount.getAndAdd(1);
        openedClosedCount.getAndAdd(1);
        return new BufCont(mark);
    }

    public static BufCont takeFromUnused(BufCont other, Mark mark) {
        Optional<DataBuffer> buf = other.takeBuf();
        if (buf.isEmpty()) {
            throw new RuntimeException("logic");
        }
        BufCont ret = fromBuffer(buf.get(), Mark.__NOMARK);
        ret.appendMarks(other);
        ret.appendMark(mark);
        return ret;
    }

    private BufCont(Mark marks) {
        id = idCount.getAndAdd(1);
        if (marks != Mark.__NOMARK) {
            appendMark(marks);
        }
        if (storeTraceAccess) {
            eCtor = new Throwable();
            refAccesses = new ArrayList<>();
        }
        if (addToAll) {
            if (allEver.size() < allEverMax) {
                allEver.put(id, this);
            }
        }
    }

    @Override
    public synchronized void close() {
        close1();
        if (inner != null) {
            if (cleanable == null) {
                closeErrorCount.getAndAdd(1);
            }
            else if (inner.buf == null) {
                closeErrorCount.getAndAdd(1);
            }
            else {
                inner.mark = true;
                int cap2 = inner.buf.capacity();
                if (cap2 != cap) {
                    closeCapMismatchCount.getAndAdd(1);
                    closeCapMismatchBytes.getAndAdd(cap2 - cap);
                }
                closedBytes.getAndAdd(cap);
                openedClosedBytes.getAndAdd(-cap);
                Cleaner.Cleanable cl = cleanable;
                cleanable = null;
                inner = null;
                cl.clean();
            }
        }
        else {
            if (cleanable != null) {
                closeErrorCount.getAndAdd(1);
            }
        }
    }

    public boolean hasBuf() {
        if (closed > 0) {
            if (eClosed != null) {
                ByteArrayOutputStream os = new ByteArrayOutputStream();
                PrintStream ps = new PrintStream(os);
                eClosed.printStackTrace(ps);
                ps.close();
                LOGGER.error("hasBuf but already closed at\n{}", os.toString(StandardCharsets.UTF_8));
            }
            String s = ErrorUtils.traceString(new Throwable(String.format("hasBuf on closed buffer  id %d", id)));
            LOGGER.error("hasBuf on closed buffer\n{}", s);
            return false;
        }
        else {
            return inner != null && inner.buf != null;
        }
    }

    public boolean isEmpty() {
        return !hasBuf();
    }

    public DataBuffer bufferRef() throws NoBufferException {
        if (closed > 0) {
            String s = ErrorUtils.traceString(new Throwable(String.format("bufferRef on closed buffer  id %d", id)));
            LOGGER.error("bufferRef on closed buffer\n{}", s);
            throw new RuntimeException("logic");
        }
        if (storeTraceAccess) {
            refAccesses.add(new Throwable());
        }
        if (hasBuf()) {
            return inner.buf;
        }
        else {
            throw new NoBufferException("bufferRef");
        }
    }

    public synchronized Optional<DataBuffer> takeBuf() {
        if (closed > 0) {
            if (hasBuf()) {
                takeClosedNotEmpty.getAndAdd(1);
            }
            else {
                takeButClosed.getAndAdd(1);
            }
            return Optional.empty();
        }
        close1();
        take1();
        Optional<DataBuffer> ret = Optional.empty();
        if (inner != null) {
            if (cleanable == null) {
                throw new RuntimeException("logic");
            }
            if (inner.buf == null) {
                throw new NoBufferException("takeBuf:inner");
            }
            DataBuffer b2 = inner.buf;
            inner.buf = null;
            inner.mark = true;
            Cleaner.Cleanable cl = cleanable;
            cleanable = null;
            inner = null;
            // TODO any performance difference?
            //cl.clean();
            ret = Optional.of(b2);
            //int cap = b2.capacity();
            closedBytes.getAndAdd(cap);
            openedClosedBytes.getAndAdd(-cap);
        }
        else {
            takeButEmpty.getAndAdd(1);
            if (cleanable != null) {
                throw new NoBufferException("takeBuf:empty");
            }
        }
        return ret;
    }

    void close1() {
        if (removeFromAllOnClose) {
            allEver.remove(id);
        }
        if (closed > 0) {
            againClosedCount.getAndAdd(1);
        }
        else {
            closedCount.getAndAdd(1);
            openedClosedCount.getAndAdd(-1);
            if (storeTraceAccess) {
                eClosed = new Throwable();
                emClosed = Mono.error(new RuntimeException("---------------   CLOSED HERE   -------------------------------"));
            }
        }
        if (closed < 120) {
            closed += 1;
        }
    }

    void take1() {
        if (taken < 120) {
            taken += 1;
        }
    }

    public BufCont cloned(Mark mark) {
        if (isEmpty()) {
            BufCont ret = makeEmpty(Mark.CLONED_EMPTY);
            ret.isCloned = true;
            return ret;
        }
        else {
            DataBuffer b1 = bufferRef();
            DataBuffer b2 = b1.retainedSlice(0, b1.capacity());
            b2.readPosition(b1.readPosition());
            b2.writePosition(b1.writePosition());
            BufCont ret = fromBuffer(b2, Mark.__NOMARK);
            ret.appendMarks(this);
            ret.appendMark(Mark.CLONED_FROM);
            ret.appendMark(mark);
            ret.isCloned = true;
            return ret;
        }
    }

    public int readableByteCount() {
        if (hasBuf()) {
            return inner.buf.readableByteCount();
        }
        else {
            return 0;
        }
    }

    public boolean closed() {
        return closed != 0;
    }

    public static String listOpen() {
        synchronized (allEver) {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            PrintStream ps = new PrintStream(os);
            ps.format("~~~~~~~~~~~~~~~~~   begin buffer trace   ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n");
            if (!storeTraceAccess) {
                ps.format("   !!!!!!!!    NO TRACE COLLECTED   !!!!!!!!!!!!!!!!!\n");
            }
            if (false) {
                AtomicInteger printedTraces = new AtomicInteger();
                allEver.forEach(Long.MAX_VALUE, (bid, bc) -> {
                    if (bc.closed == 0 && bc.inner != null && bc.inner.buf != null) {
                        if (printedTraces.getAndAdd(1) < 3) {
                            ps.format("!!!!!!!!!!!!!!!!!!\nBufCont name: %s\nCREATED AT:\n", bc.marksToString());
                            if (bc.eCtor != null) {
                                ps.flush();
                                bc.eCtor.printStackTrace(ps);
                            }
                            if (bc.refAccesses != null) {
                                AtomicInteger i1 = new AtomicInteger();
                                bc.refAccesses.forEach(e -> {
                                    ps.format("~~~~~~~~~~~~~~~~~~~~~~~   ACCESS %d\n", i1.getAndAdd(1));
                                    ps.flush();
                                    e.printStackTrace(ps);
                                });
                            }
                            ps.format("~~~~~~~~~~~~~~~~~~~~~~~   END   ~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n");
                        }
                    }
                });
            }
            AtomicInteger openCount = new AtomicInteger();
            AtomicLong openBytes = new AtomicLong();
            AtomicInteger openNotMarkedCount = new AtomicInteger();
            allEver.forEach(Long.MAX_VALUE, (bid, bc) -> {
                int cap = -1;
                BufCont.Inner inner = bc.inner;
                if (inner != null) {
                    cap = -2;
                    if (inner.buf != null) {
                        cap = inner.buf.capacity();
                    }
                    if (!inner.mark) {
                        openNotMarkedCount.getAndAdd(1);
                    }
                }
                if (bc.closed == 0) {
                    openCount.getAndAdd(1);
                    if (cap > 0) {
                        openBytes.getAndAdd(cap);
                    }
                }
                if (true || bc.closed == 0) {
                    long age = (System.nanoTime() - bc.ts) / 1000 / 1000 / 1000;
                    ps.format("id %19d  name %s  closed %2d  taken %2d  cap %6d  age %6d\n", bc.id, bc.marksToString(), bc.closed, bc.taken, cap, age);
                }
            });
            ps.format("allEver.size %d\n", allEver.size());
            ps.format("openNotMarkedCount %d\n", openNotMarkedCount.get());
            ps.format("openCount %d\n", openCount.get());
            ps.format("openBytes %d\n", openBytes.get());
            ps.format("~~~~~~~~~~~~~~~~~   end buffer trace   ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n");
            ps.close();
            return os.toString(StandardCharsets.UTF_8);
        }
    }

    static void initMarkHist() {
        Mark[] marks = Mark.values();
        for (int i = 1; i < marks.length; i += 1) {
            if (marks[i-1].compareTo(marks[i]) >= 0) {
                LOGGER.error("initMarkHist not sorted");
                throw new RuntimeException("initMarkHist not sorted");
            }
        }
        int n = marks.length;
        markHist = new AtomicLongArray(n * n);
    }

    public static List<String> markValues() {
        return Arrays.stream(Mark.values()).map(Objects::toString).collect(Collectors.toList());
    }

    public static List<Long> markHistCounts() {
        return LongStream.range(0, markHist.length()).map(i -> markHist.get((int) i)).boxed().collect(Collectors.toList());
    }

    public static void gc() {
        long tsNow = System.nanoTime();
        allEver.forEach((bid, bc) -> {
            if (tsNow - bc.ts > 1000L * 1000 * 1000 * 60 * 60) {
                BufCont bc2 = allEver.remove(bid);
                bc2.addToMarkHist();
                bc2.close();
            }
        });
        gcCount.getAndAdd(1);
    }

    void addToMarkHist() {
        Mark m1 = Mark.__NOMARK;
        Mark m2 = Mark.__NOMARK;
        if (markCount >= 2) {
            m1 = marks[(markIx + marks.length - 1) % marks.length];
            m2 = marks[(markIx + marks.length - 2) % marks.length];
        }
        else if (markCount == 1) {
            m1 = marks[(markIx + marks.length - 1) % marks.length];
        }
        int j1 = markIx(m1);
        int j2 = markIx(m2);
        if (j1 >= 0 && j2 >= 0 && j1 < marks.length && j2 < marks.length) {
            int ix = j2 * marks.length + j1;
            if (ix < markHist.length()) {
                BufCont.markHist.getAndAdd(ix, 1);
            }
        }
    }

    int markIx(Mark mark) {
        int k = Arrays.binarySearch(Mark.values(), mark);
        if (k < 0) {
            errorCount.getAndAdd(1);
            return -1;
        }
        else if (k >= Mark.values().length) {
            errorCount.getAndAdd(1);
            return -1;
        }
        else {
            return k;
        }
    }

    public void appendMark(Mark k) {
        if (doMark) {
            synchronized (this) {
                if (markCount < 128) {
                    marks[markIx] = k;
                    markCount += 1;
                    markIx = (markIx + 1) % marks.length;
                }
            }
        }
    }

    public void appendMarks(BufCont other) {
        if (doMark) {
            synchronized (this) {
                synchronized (other) {
                    int beg = 0;
                    int n1 = other.markCount;
                    if (other.markCount > other.marks.length) {
                        beg = other.markIx;
                        n1 = other.marks.length;
                    }
                    int p1 = beg;
                    for (int i1 = 0; i1 < n1; i1 += 1) {
                        appendMark(other.marks[p1]);
                        p1 = (p1 + 1) % other.marks.length;
                    }
                }
            }
        }
    }

    public static BufCont fromString(CharBuffer s) {
        BufCont bc = BufCont.allocate(DefaultDataBufferFactory.sharedInstance, s.length() + 32, BufCont.Mark.__NOMARK);
        bc.bufferRef().write(s, StandardCharsets.UTF_8);
        return bc;
    }

    public static BufCont fromString(StringBuilder s) {
        BufCont bc = BufCont.allocate(DefaultDataBufferFactory.sharedInstance, s.length() + 32, BufCont.Mark.__NOMARK);
        bc.bufferRef().write(s, StandardCharsets.UTF_8);
        return bc;
    }

    public static BufCont fromString(String s) {
        BufCont bc = BufCont.allocate(DefaultDataBufferFactory.sharedInstance, s.length() + 32, BufCont.Mark.__NOMARK);
        bc.bufferRef().write(s, StandardCharsets.UTF_8);
        return bc;
    }

    public String marksToString() {
        StringBuilder sb = new StringBuilder();
        int beg = 0;
        int n1 = markCount;
        if (markCount > marks.length) {
            beg = markIx;
            n1 = marks.length;
        }
        int p1 = beg;
        for (int i1 = 0; i1 < n1; i1 += 1) {
            sb.append(marks[p1].toString()).append(" ");
            p1 = (p1 + 1) % marks.length;
        }
        return sb.toString();
    }

    public void releaseFinite() {
        close();
    }

    static List<String> hexFrag;
    static {
        hexFrag = IntStream.range(0, 256).boxed().map(k -> String.format("%02x", k)).collect(Collectors.toList());
    }

    public static StringBuilder dumpContent(DataBuffer buf) {
        return dumpContent(buf, 0);
    }

    public static StringBuilder dumpContent(BufCont bc) {
        if (bc.hasBuf()) {
            DataBuffer buf = bc.bufferRef();
            return dumpContent(buf, 0);
        }
        else {
            return new StringBuilder("[BufCont empty]");
        }
    }

    public static StringBuilder dumpContent(DataBuffer buf, int nnnn) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("cap %d  rp %d  wp %d\n", buf.capacity(), buf.readPosition(), buf.writePosition()));
        int i1;
        for (i1 = buf.readPosition(); i1 < buf.writePosition(); i1 += 1) {
            int p = 0xff & buf.getByte(i1);
            sb.append(hexFrag.get(p)).append(" ");
            if (i1 % 16 == 15) {
                sb.append("\n");
            }
        }
        if (i1 % 16 != 0) {
            sb.append("\n");
        }
        return sb;
    }

}
