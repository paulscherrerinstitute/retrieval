package ch.psi.daq.retrieval.merger;

import ch.psi.daq.retrieval.ReqCtx;
import ch.psi.daq.retrieval.eventmap.ts.Item;
import ch.psi.daq.retrieval.eventmap.ts.ItemP;
import ch.qos.logback.classic.Logger;
import io.netty.buffer.ByteBuf;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.springframework.core.io.buffer.*;
import reactor.core.publisher.Flux;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class Merger implements Publisher<DataBuffer> {
    static Logger LOGGER = (Logger) LoggerFactory.getLogger(Merger.class);
    static Marker markerNesting = MarkerFactory.getMarker("MergerNesting");
    String channelName;
    boolean doTrace = LOGGER.isTraceEnabled();
    List<Flux<Item>> inp;
    int nsubscribed;
    long nreqd;
    Subscriber<? super DataBuffer> scrd;
    List<MergerSubscriber> scrs = new ArrayList<>();
    int state;
    int inpix;
    long lastTs;
    DataBuffer cbuf;
    DataBufferFactory bufFac;
    DataBufferFactory bufFac2;
    int bufferSize;
    int inAssemble;
    int redoAssemble;
    boolean cancelled;
    boolean finalCompleteDone;
    boolean released;
    long totalSeenBytesFromUpstream;
    long writtenBytes;
    boolean requireBufferType = false;
    int chunkEmit = Integer.MIN_VALUE;
    int chunkExpect = Integer.MIN_VALUE;
    int assembleInnerReturn = -1;
    int assembleInnerBreakReason = -1;
    ReqCtx reqctx;

    enum WriteType {
        FULL,
        BEGIN,
        BLOB,
        END,
    }

    static public class Written {
        int uix;
        int pos;
        int end;
        long ts;
        WriteType wty;
        public Written(int uix, int pos, int end, long ts, WriteType wty) {
            this.uix = uix;
            this.pos = pos;
            this.end = end;
            this.ts = ts;
            this.wty = wty;
        }
        public String toString() {
            return String.format("Written { uix %2d  pos %10d  end %10d  ts %16d  wty %5s }", uix, pos, end, ts, wty);
        }
    }

    List<Written> writtenLog = new ArrayList<>();

    public Merger(ReqCtx reqctx, String channelName, List<Flux<Item>> inp, DataBufferFactory bufFac, int bufferSize) {
        this.reqctx = reqctx;
        this.channelName = channelName;
        this.inp = inp;
        this.bufFac2 = bufFac;
        this.bufFac = new DefaultDataBufferFactory();
        this.bufferSize = 3 * bufferSize;
        this.cbuf = bufFac.allocateBuffer(bufferSize);
        if (!checkBuf(this.cbuf)) {
            throw new RuntimeException("checkBuf");
        }
        for (int i3 = 0; i3 < inp.size(); i3 += 1) {
            MergerSubscriber scr = new MergerSubscriber(this, channelName, i3);
            scrs.add(scr);
        }
        for (int i3 = 0; i3 < inp.size(); i3 += 1) {
            Flux<Item> fl = inp.get(i3);
            fl.subscribe(scrs.get(i3));
        }
    }

    boolean checkBuf(DataBuffer buf) {
        if (buf == null) {
            LOGGER.error("{}  checkBuf  buf == null", reqctx);
            selfError(new RuntimeException("checkBuf"));
            return false;
        }
        if (buf instanceof NettyDataBuffer) {
            ByteBuf b1 = ((NettyDataBuffer) buf).getNativeBuffer();
            if (b1.refCnt() < 1) {
                LOGGER.error("{}  checkBuf  unexpected count {}", reqctx, b1.refCnt());
                selfError(new RuntimeException("checkBuf"));
                return false;
            }
        }
        else if (requireBufferType) {
            LOGGER.error("{}  checkBuf  unexpected buffer type", reqctx);
            selfError(new RuntimeException("checkBuf"));
            return false;
        }
        return true;
    }

    boolean isWritableBuffer(DataBuffer buf) {
        if (buf instanceof NettyDataBuffer) {
            NettyDataBuffer b1 = (NettyDataBuffer) buf;
            try {
                b1.getNativeBuffer().ensureWritable(1);
                return true;
            }
            catch (Throwable e) {
                LOGGER.error("{}  not isWritableBuffer", reqctx);
                return false;
            }
        }
        else {
            return true;
        }
    }

    @Override
    public synchronized void subscribe(Subscriber<? super DataBuffer> scr) {
        if (nsubscribed > 0) {
            throw new RuntimeException("logic");
        }
        nsubscribed += 1;
        this.scrd = scr;
        this.scrd.onSubscribe(new MergerSubscription(this));
    }

    public synchronized void request(long n, int inReq) {
        LOGGER.trace(markerNesting, "{}  Merger::request  BEGIN  n {}  inReq {}  nreqd {}", reqctx, n, inReq, nreqd);
        if (n == Long.MAX_VALUE) {
            if (nreqd != 0) {
                LOGGER.warn("{}  Request unbounded even though previous request  nreqd: {}", reqctx, nreqd);
            }
            nreqd = Long.MAX_VALUE;
        }
        else if (n < 1) {
            LOGGER.error("{}  request n {}", reqctx, n);
            selfError(new RuntimeException("logic"));
        }
        else if (n > Long.MAX_VALUE / 3 && nreqd > Long.MAX_VALUE / 3) {
            LOGGER.error("{}  too large bounded request {}", reqctx, n);
            selfError(new RuntimeException("logic"));
        }
        else {
            nreqd += n;
        }
        LOGGER.trace(markerNesting, "{}  Merger::request  DO     n {}  inReq {}  nreqd {}", reqctx, n, inReq, nreqd);
        if (inReq < 0) {
            LOGGER.error("{}  inReq {}", reqctx, inReq);
            selfError(new RuntimeException("logic"));
        }
        else if (inReq == 0) {
            assemble(-1);
        }
        else if (inAssemble <= 0) {
            LOGGER.error("{}  inAssemble {}", reqctx, inAssemble);
            selfError(new RuntimeException("logic"));
        }
        LOGGER.trace(markerNesting, "{}  Merger::request  END    n {}  inReq {}  nreqd {}", reqctx, n, inReq, nreqd);
    }

    public synchronized void cancel() {
        if (cbuf == null) {
            LOGGER.error("{}  cbuf already null on cancel", reqctx);
        }
        cancelled = true;
        for (MergerSubscriber sub : scrs) {
            sub.cancel();
        }
        release();
    }

    synchronized void release() {
        LOGGER.debug("{}  release begin", reqctx);
        for (MergerSubscriber scr : scrs) {
            scr.release();
        }
        if (cbuf != null) {
            DataBufferUtils.release(cbuf);
            cbuf = null;
        }
        released = true;
        LOGGER.debug("{}  release done", reqctx);
    }

    public synchronized void next(int id) {
        long nrequ = nrequ();
        LOGGER.trace(markerNesting, "{}  Merger::next  BEGIN  id {}  nrequ {}", reqctx, id, nrequ);
        if (cancelled) {
            LOGGER.warn("{}  next called even though cancelled  id: {}", reqctx, id);
            return;
        }
        if (released) {
            LOGGER.warn("{}  next called even though released  id: {}", reqctx, id);
            return;
        }
        try {
            LOGGER.trace("{}  {}  next  nrequ: {}", reqctx, id, nrequ);
            if (nrequ == 0) {
                assemble(id);
            }
            else {
                LOGGER.trace("{}  {}  request waiting for outstanding upstream", reqctx, id);
            }
        }
        catch (Throwable e) {
            LOGGER.error("{}  {}  err in next: {}", reqctx, id, e.toString());
            selfError(e);
        }
        LOGGER.trace(markerNesting, "{}  Merger::next  END    id {}  nrequ {}", reqctx, id, nrequ);
    }

    public synchronized void signal(int id) {
        long nrequ = nrequ();
        LOGGER.trace(markerNesting, "{}  Merger::signal  BEGIN  id {}  nrequ {}", reqctx, id, nrequ);
        if (cancelled) {
            LOGGER.warn("{}  signal called even though cancelled  id: {}", reqctx, id);
            return;
        }
        if (released) {
            LOGGER.warn("{}  signal called even though released  id: {}", reqctx, id);
            return;
        }
        try {
            if (nrequ == 0) {
                assemble(id);
            }
            else {
                LOGGER.debug("{}  {}  signal waiting for outstanding upstream", reqctx, id);
            }
        }
        catch (Throwable e) {
            LOGGER.error("{}  {}  err in signal: {}", reqctx, e.toString(), id);
            selfError(e);
        }
        LOGGER.trace(markerNesting, "{}  Merger::signal  END    id {}  nrequ {}", reqctx, id, nrequ);
    }

    synchronized void assemble(int id) {
        LOGGER.trace(markerNesting, "{}  Merger::assemble  BEGIN  id {}  redoAssemble {}", reqctx, id, redoAssemble);
        if (cancelled) {
            LOGGER.warn("{}  in assemble():  already cancelled, no need to assemble", reqctx);
            return;
        }
        if (cbuf == null) {
            if (finalCompleteDone) {
                LOGGER.debug("{}  cbuf null, but already complete anyway", reqctx);
                return;
            }
            else {
                LOGGER.error("{}  in assemble but cbuf null", reqctx);
                throw new RuntimeException("logic");
            }
        }
        if (inAssemble > 0) {
            LOGGER.trace(markerNesting, "{}  Merger::assemble  SKIP   id {}  redoAssemble {}", reqctx, id, redoAssemble);
            redoAssemble += 1;
        }
        else {
            try {
                inAssemble += 1;
                assembleInner(id);
                while (redoAssemble != 0) {
                    if (redoAssemble < 0) {
                        LOGGER.error("{}  redoAssemble < 0", reqctx);
                        selfError(new RuntimeException("redoAssemble < 0"));
                        return;
                    }
                    LOGGER.trace(markerNesting, "{}  Merger::assemble  REDO   id {}  redoAssemble {}", reqctx, id, redoAssemble);
                    redoAssemble -= 1;
                    assembleInner(id);
                }
            }
            finally {
                inAssemble -= 1;
            }
            LOGGER.trace(markerNesting, "{}  Merger::assemble  END    id {}  redoAssemble {}", reqctx, id, redoAssemble);
        }
    }

    void assembleInner(int id) {
        LOGGER.trace(markerNesting, "{}  Merger::assembleInner  BEGIN  id {}  nreqd {}", reqctx, id, nreqd);
        assembleInnerReturn = -1;
        assembleInnerBreakReason = -1;
        if (nrequ() != 0) {
            LOGGER.debug("{}  assembleInner request waiting for outstanding upstream", reqctx);
            return;
        }
        if (cbuf == null) {
            LOGGER.error("{}  cbuf null  finalCompleteDone {}", reqctx, finalCompleteDone);
            return;
        }
        while (!cancelled && nreqd > 0) {
            if (writtenLog.size() > 32) {
                int i2 = writtenLog.size() - 4;
                for (int i1 = 0; i1 < 4; i1 += 1) {
                    writtenLog.set(i1, writtenLog.get(i2));
                    i2 += 1;
                }
                while (writtenLog.size() > 4) {
                    writtenLog.remove(writtenLog.size() - 1);
                }
            }
            LOGGER.trace("{}  assemble loop  state: {}  inpix: {}", reqctx, state, inpix);
            for (int i1 = 0; i1 < scrs.size(); i1 += 1) {
                MergerSubscriber scr = scrs.get(i1);
                LOGGER.trace("scr  i1 {}  term {}  item {}", i1, scr.itemTerm, scr.item);
                while (scr.item != null && !scr.item.isPlainBuffer() && !scr.item.hasMoreMarkers()) {
                    scr.itemAdvOrRemove();
                    LOGGER.debug("{}  useless item, advanced, now item {} {}", reqctx, i1, scr.item);
                }
            }
            if (nreqd <= 0) {
                LOGGER.warn("{}  Potential 2 upcoming write with nreqd: {}", reqctx, nreqd);
            }
            if (state == 0) {
                long tsm = Long.MAX_VALUE;
                int i2 = -1;
                int validTsCompared = 0;
                int nNoMoreItems = 0;
                int nItemNull = 0;
                int nItem1Null = 0;
                int nItemNullMaybeMoreItems = 0;
                int nNeedsMoreMaybeMoreItems = 0;
                int nNoPositionsMarkedMaybeMoreItems = 0;
                int nNoPositionsMarked = 0;
                for (int i1 = 0; i1 < scrs.size(); i1 += 1) {
                    MergerSubscriber scr = scrs.get(i1);
                    if (!scr.maybeMoreItems()) {
                        nNoMoreItems += 1;
                    }
                    if (scr.item == null) {
                        if (scr.maybeMoreItems()) {
                            nItemNullMaybeMoreItems += 1;
                        }
                        else {
                            nItemNull += 1;
                        }
                    }
                    else {
                        if (!scr.hasMoreMarkers() && scr.maybeMoreItems()) {
                            nNeedsMoreMaybeMoreItems += 1;
                        }
                        else if (scr.item.item1 == null) {
                            nItem1Null += 1;
                        }
                        else if (scr.item.item1.c < 0) {
                            LOGGER.error("{}  unexpected c {}", reqctx, scr.item.item1.c);
                            selfError(new RuntimeException("weird c"));
                        }
                        else if (scr.item.item1.c == 0) {
                            if (scr.maybeMoreItems()) {
                                nNoPositionsMarkedMaybeMoreItems += 1;
                                scr.releaseItem();
                            }
                            else {
                                nNoPositionsMarked += 1;
                            }
                        }
                        else if (scr.item.ix < 0 || scr.item.ix >= scr.item.item1.c) {
                            LOGGER.error("{}  unexpected ix {}", reqctx, scr.item.ix);
                            selfError(new RuntimeException("weird ix"));
                            return;
                        }
                        else {
                            if (scr.item.item1.ty[scr.item.ix] != 1) {
                                LOGGER.error("{}  unexpected ty {}", reqctx, scr.item.item1.ty[scr.item.ix]);
                                selfError(new RuntimeException("logic"));
                                return;
                            }
                            long ts = scr.item.item1.ts[scr.item.ix];
                            if (ts < tsm) {
                                tsm = ts;
                                i2 = i1;
                            }
                            validTsCompared += 1;
                        }
                    }
                }
                if (tsm == Long.MAX_VALUE) {
                    tsm = 0;
                }
                if (doTrace) {
                    LOGGER.trace("{}  tsm {} {}  .. validTsCompared {}  nItemNull {}  nItemNullMaybeMoreItems {}  nNeedsMoreMaybeMoreItems {}  nItem1Null {}  nNoMoreItems {}  nNoPositionsMarkedMaybeMoreItems {}  nNoPositionsMarked {}",
                    reqctx, tsm / 1000000000L, tsm % 1000000000L, validTsCompared, nItemNull, nItemNullMaybeMoreItems, nNeedsMoreMaybeMoreItems, nItem1Null, nNoMoreItems, nNoPositionsMarkedMaybeMoreItems, nNoPositionsMarked);
                }
                if (nItemNullMaybeMoreItems > 0) {
                    i2 = -1;
                }
                if (nNeedsMoreMaybeMoreItems > 0) {
                    i2 = -1;
                }
                if (nNoPositionsMarkedMaybeMoreItems > 0) {
                    i2 = -1;
                }
                if (i2 < 0) {
                    LOGGER.trace(markerNesting, "{}  choice not possible", reqctx);
                    assembleInnerBreakReason = 1;
                    break;
                }
                inpix = i2;
                MergerSubscriber fscr = scrs.get(i2);
                int six = fscr.item.ix;
                if (fscr.item.item1.buf == null) {
                    LOGGER.error("{}  buf null", reqctx);
                    selfError(new RuntimeException("logic"));
                    return;
                }
                if (nreqd <= 0) {
                    LOGGER.warn("{}  potential upcoming write with nreqd: {}", reqctx, nreqd);
                }
                if (six + 1 < fscr.item.item1.c) {
                    if (fscr.item.item1.ty[six + 1] != 2) {
                        LOGGER.error("{}  unexpected ty {}", reqctx, fscr.item.item1.ty[six + 1]);
                        selfError(new RuntimeException("logic"));
                        return;
                    }
                    if (fscr.item.item1.ts[six + 1] != tsm) {
                        LOGGER.error("{}  unexpected ts {}", reqctx, fscr.item.item1.ts[six + 1]);
                        selfError(new RuntimeException("logic"));
                        return;
                    }
                    DataBuffer buf = fscr.item.item1.buf;
                    int pos1 = fscr.item.item1.pos[six];
                    int pos2 = fscr.item.item1.pos[six + 1];
                    int pos = pos1;
                    int n = pos2 - pos1;
                    long ts = fscr.item.item1.ts[six];
                    ByteBuffer bb = buf.asByteBuffer(0, buf.capacity());
                    int len1 = bb.getInt(pos);
                    LOGGER.trace("{}  full chunk in same buffer  inpix {}  six {}  pos {}  ts {}  n {}  len1 {}", reqctx, inpix, six, pos, ts, n, len1);
                    int len2 = bb.getInt(pos + len1 - 4);
                    if (len1 != len2) {
                        LOGGER.error("{}  len mismatch  {} vs {}", reqctx, len1, len2);
                        dumpState();
                        selfError(new RuntimeException("bad"));
                        return;
                    }
                    chunkExpect = len1;
                    if (chunkExpect != n) {
                        LOGGER.error("{}  full chunk in same buffer copy  six {}  pos {}  n {}  chunkExpect {}", reqctx, six, pos, n, chunkExpect);
                        dumpState();
                        selfError(new RuntimeException("bad"));
                        return;
                    }
                    if (!writeOutput(buf, pos, n)) {
                        assembleInnerReturn = 1;
                        return;
                    }
                    writtenLog.add(new Written(inpix, pos, pos + n, tsm, WriteType.FULL));
                    chunkExpect = Integer.MIN_VALUE;
                    chunkEmit = Integer.MIN_VALUE;
                    fscr.itemAdvOrRemove();
                    fscr.itemAdvOrRemove();
                }
                else {
                    DataBuffer buf = fscr.item.item1.buf;
                    int pos = fscr.item.item1.pos[six];
                    long ts = fscr.item.item1.ts[six];
                    int n = fscr.item.item1.p2 - pos;
                    ByteBuffer bb = buf.asByteBuffer(0, buf.capacity());
                    int len1 = bb.getInt(pos);
                    LOGGER.trace("{}  begin chunk  inpix {}  six {}  pos {}  ts {}  n {}  len1 {}", reqctx, inpix, six, pos, ts, n, len1);
                    chunkExpect = len1;
                    if (nreqd <= 0) {
                        selfError(new RuntimeException(String.format("about to call writeOutput with nreqd: %d", nreqd)));
                        return;
                    }
                    chunkEmit = n;
                    if (!writeOutput(buf, pos, n)) {
                        assembleInnerReturn = 2;
                        return;
                    }
                    writtenLog.add(new Written(inpix, pos, pos + n, tsm, WriteType.BEGIN));
                    fscr.itemAdvOrRemove();
                    LOGGER.trace("{}  after begin chunk and advance  {}", reqctx, fscr.item);
                    state = 1;
                    lastTs = tsm;
                }
            }
            else if (state == 1) {
                MergerSubscriber fscr = scrs.get(inpix);
                LOGGER.trace("{}  state {}  inpix {}  item {}", reqctx, state, inpix, fscr != null ? fscr.item : null);
                if (fscr == null) {
                    LOGGER.error("{}  MergerSubscriber vanished", reqctx);
                    selfError(new RuntimeException("MergerSubscriber vanished"));
                    return;
                }
                if (!fscr.hasItem()) {
                    LOGGER.trace(markerNesting, "{}  state {}  inpix {}  NO ITEM  breaking", reqctx, state, inpix);
                    assembleInnerBreakReason = 2;
                    break;
                }
                Item item = fscr.getItem();
                if (item.isPlainBuffer()) {
                    ItemP item1 = item.item1;
                    DataBuffer buf = item1.buf;
                    int pos = item1.p1;
                    int n = item1.p2 - item1.p1;
                    LOGGER.trace("{}  plain buffer inpix {}  pos {}  n {}  buf {}", reqctx, inpix, pos, n, buf);
                    if (nreqd <= 0) {
                        selfError(new RuntimeException(String.format("about to call writeOutput with nreqd: %d", nreqd)));
                        return;
                    }
                    chunkEmit += n;
                    if (!writeOutput(buf, pos, n)) {
                        assembleInnerReturn = 3;
                        return;
                    }
                    writtenLog.add(new Written(inpix, pos, pos + n, lastTs, WriteType.BLOB));
                    fscr.itemAdvOrRemove();
                }
                else if (fscr.hasMoreMarkers()) {
                    int six = fscr.item.ix;
                    if (fscr.item.item1.ty[six] != 2) {
                        selfError(new RuntimeException("logic"));
                        return;
                    }
                    if (fscr.item.item1.ts[six] != lastTs) {
                        selfError(new RuntimeException("logic"));
                        return;
                    }
                    DataBuffer buf = fscr.item.item1.buf;
                    int pos = fscr.item.item1.p1;
                    int n = fscr.item.item1.pos[six] - pos;
                    long ts = fscr.item.item1.ts[six];
                    int len2 = buf.asByteBuffer(0, buf.capacity()).getInt(pos + n - 4);
                    if (len2 != chunkExpect) {
                        LOGGER.error("{}  terminal len mismatch  chunkExpect {}  len2 {}", reqctx, chunkExpect, len2);
                        dumpState();
                        selfError(new RuntimeException("bad"));
                        return;
                    }
                    if (nreqd <= 0) {
                        selfError(new RuntimeException(String.format("about to call writeOutput with nreqd: %d", nreqd)));
                        return;
                    }
                    chunkEmit += n;
                    if (chunkEmit != chunkExpect) {
                        LOGGER.error("{}  chunkExpect != chunkEmit  chunkExpect {}  chunkEmit {}", reqctx, chunkExpect, chunkEmit);
                        dumpState();
                        selfError(new RuntimeException("bad"));
                        assembleInnerReturn = 13;
                        return;
                    }
                    if (!writeOutput(buf, pos, n)) {
                        assembleInnerReturn = 4;
                        return;
                    }
                    writtenLog.add(new Written(inpix, pos, pos + n, ts, WriteType.END));
                    LOGGER.trace(markerNesting, "{}  terminated buffer  inpix {}  pos {}  n {}  ts {}  buf {}", reqctx, inpix, pos, n, ts, buf);
                    fscr.itemAdvOrRemove();
                    chunkExpect = Integer.MIN_VALUE;
                    chunkEmit = Integer.MIN_VALUE;
                    state = 0;
                    inpix = -1;
                    lastTs = -1;
                }
                else {
                    LOGGER.error("{}  state {}  no plain, no has more", reqctx, state);
                    selfError(new RuntimeException(String.format("state %d  no plain, no has more", state)));
                    return;
                }
            }
            else {
                LOGGER.error("{}  unexpected state {}", reqctx, state);
                scrd.onError(new RuntimeException("logic"));
                assembleInnerReturn = 13;
                return;
            }
        }
        if (cancelled) {
            LOGGER.debug("{}  assembleInner  do not check for refill because cancelled", reqctx);
            assembleInnerReturn = 2;
            return;
        }
        if (nreqd > 0) {
            int nReqUp = 0;
            for (int i1 = 0; i1 < scrs.size(); i1 += 1) {
                MergerSubscriber scr = scrs.get(i1);
                if (!scr.hasItem() && scr.maybeMoreItems()) {
                    LOGGER.debug("{}  request next item for {}  item null: {}  isPlainBuffer: {}  isTerm: {}",
                    reqctx, i1, scr.item == null, scr.item != null && scr.item.isPlainBuffer(), scr.item != null && scr.item.isTerm());
                    scr.request();
                    nReqUp += 1;
                }
            }
            if (nReqUp == 0) {
                LOGGER.trace(markerNesting, "{}  Merger  refill  nReqUp == 0", reqctx);
                boolean allCompleteAndEmpty = scrs.stream().allMatch(scr -> !scr.hasItem() && !scr.maybeMoreItems());
                if (allCompleteAndEmpty) {
                    LOGGER.trace(markerNesting, "{}  Merger  refill  nReqUp == 0 && allCompleteAndEmpty", reqctx);
                    finalComplete();
                    if (redoAssemble != 0) {
                        LOGGER.trace(markerNesting, "{}  Merger  refill  nReqUp == 0 && allCompleteAndEmpty  RESET REDO COUNT", reqctx);
                        redoAssemble = 0;
                    }
                }
                else {
                    LOGGER.trace(markerNesting, "{}  Merger  refill  nReqUp == 0 && !allCompleteAndEmpty", reqctx);
                    dumpState();
                }
            }
        }
        assembleInnerReturn = 0;
        LOGGER.trace(markerNesting, "{}  Merger::assembleInner  END    id {}  nreqd {}", reqctx, id, nreqd);
    }

    long writeOutputN = 0;

    synchronized boolean writeOutput(DataBuffer src, int pos, int n) {
        writeOutputN += 1;
        if (!checkBuf(cbuf)) {
            LOGGER.error("{}  checkBuf error", reqctx);
            selfError(new RuntimeException("checkBuf error"));
            return false;
        }
        if (cancelled) {
            LOGGER.error("{}  writeOutput despite cancelled", reqctx);
            return false;
        }
        if (src == null) {
            LOGGER.error("{}  writeOutput with src null", reqctx);
            selfError(new RuntimeException("writeOutput with src null"));
            return false;
        }
        if (nreqd <= 0) {
            LOGGER.error("{}  writeOutput called even without nreqd", reqctx);
            selfError(new RuntimeException("writeOutput called even without nreqd"));
            return false;
        }
        if (n > cbuf.writableByteCount()) {
            LOGGER.trace("{}  writeOutput  not enough space  {}  {}", reqctx, n, cbuf.writableByteCount());
            DataBuffer xbuf = cbuf;
            cbuf = bufFac.allocateBuffer(bufferSize);
            LOGGER.trace(markerNesting, "{}  Merger  onNext  item to downstream  BEGIN  writeOutputN {}", reqctx, writeOutputN);
            nreqd -= 1;
            scrd.onNext(xbuf);
            LOGGER.trace(markerNesting, "{}  Merger  onNext  item to downstream  END    writeOutputN {}", reqctx, writeOutputN);
            if (!isWritableBuffer(cbuf)) {
                LOGGER.error("{}  buffer not writable", reqctx);
                selfError(new RuntimeException("buffer not writable"));
                return false;
            }
        }
        if (cancelled) {
            LOGGER.warn("{}  writeOutput  downstream has cancelled, return early", reqctx);
            return false;
        }
        if (n > cbuf.writableByteCount()) {
            if (cbuf.capacity() > 50 * 1024 * 1024) {
                LOGGER.error("{}  cbuf still too small", reqctx);
                selfError(new RuntimeException("cbuf still too small"));
                return false;
            }
            else {
                int bs0 = bufferSize;
                while (bufferSize <= 50 * 1024 * 1024 && bufferSize < n + bs0) {
                    bufferSize *= 2;
                }
                cbuf.ensureCapacity(bufferSize);
                if (n > cbuf.writableByteCount()) {
                    LOGGER.error("{}  can not allocate enough space to write  n: {}", reqctx, n);
                    selfError(new RuntimeException("can not allocate space to write"));
                    return false;
                }
            }
        }
        DataBuffer sl = src.slice(pos, n);
        cbuf.write(sl);
        writtenBytes += sl.readableByteCount();
        return true;
    }

    synchronized void finalComplete() {
        LOGGER.debug("{}  finalComplete  writtenBytes {}", reqctx, writtenBytes);
        finalCompleteDone = true;
        if (cbuf != null) {
            if (nreqd > 0) {
                DataBuffer xbuf = cbuf;
                cbuf = null;
                nreqd -= 1;
                scrd.onNext(xbuf);
            }
            else {
                LOGGER.error("{}  ignored bytes {}", reqctx, cbuf.readableByteCount());
            }
        }
        else {
            LOGGER.info("{}  finalComplete, no data to flush", reqctx);
        }
        release();
        scrd.onComplete();
        LOGGER.debug("{}  finalComplete return", reqctx);
    }

    synchronized void selfError(Throwable e) {
        StringBuilder sb = new StringBuilder();
        formatState(sb);
        LOGGER.error("{}  selfError  {}", reqctx, sb.toString());
        for (MergerSubscriber scr : scrs) {
            scr.cancel();
        }
        if (scrd != null) {
            scrd.onError(e);
        }
        else {
            LOGGER.error("{}  can not signal error {}", reqctx, e.toString());
        }
        LOGGER.error("{}  selfError return", reqctx);
    }

    long nrequ() {
        return scrs.stream().map(MergerSubscriber::nreq).reduce(0L, Long::sum);
    }

    public synchronized void formatSubscribers(StringBuilder sb) {
        for (MergerSubscriber sub : scrs) {
            sub.formatState(sb);
            sb.append("\n\n");
        }
    }

    void formatWrittenLog(StringBuilder sb) {
        for (Written w : writtenLog) {
            sb.append(w.toString()).append("\n");
        }
    }

    public synchronized void formatState(StringBuilder sb) {
        sb.append(String.format("writtenBytes %d  nreqd %d  nrequ %d  assembleInnerReturn %d  assembleInnerBreakReason %d  redoAssemble %d  cancelled %s", writtenBytes, nreqd, nrequ(), assembleInnerReturn, assembleInnerBreakReason, redoAssemble, cancelled)).append("\n");
        formatSubscribers(sb);
        formatWrittenLog(sb);
    }

    void dumpState() {
        StringBuilder sb = new StringBuilder();
        formatState(sb);
        LOGGER.error("{}  Dump Merger State\n{}", reqctx, sb.toString());
    }

}
