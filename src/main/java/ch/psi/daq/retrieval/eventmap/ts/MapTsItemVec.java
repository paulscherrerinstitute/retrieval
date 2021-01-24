package ch.psi.daq.retrieval.eventmap.ts;

import ch.psi.daq.retrieval.bytes.BufCont;
import ch.psi.daq.retrieval.merger.Releasable;
import org.springframework.core.io.buffer.DataBuffer;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class MapTsItemVec implements Releasable {
    ItemsBuffer item1;
    ItemsBuffer item2;
    boolean term;
    int bufCount;

    static class ItemsBuffer {
        BufCont bufcont;
        List<Integer> posL = new ArrayList<>();
        List<Long> tsL = new ArrayList<>();
        List<Ty> tyL = new ArrayList<>();
        List<Integer> lenL = new ArrayList<>();
        ItemsBuffer(BufCont bufcont) {
            this.bufcont = bufcont.cloned(BufCont.Mark.ITEM_VEC_IB_CTOR);
        }
        void add(int pos, int len, long ts, Ty ty) {
            posL.add(pos);
            lenL.add(len);
            tsL.add(ts);
            tyL.add(ty);
        }
        synchronized void release() {
            if (bufcont != null) {
                BufCont k = bufcont;
                bufcont = null;
                k.close();
            }
        }
    }

    public static MapTsItemVec empty() {
        return new MapTsItemVec();
    }

    public static MapTsItemVec term() {
        MapTsItemVec ret = empty();
        ret.term = true;
        return ret;
    }

    public enum Ty {
        FULL,
        OPEN,
        CLOSE,
        MIDDLE,
    }

    public void bufferBegin(BufCont bufcont) {
        if (bufCount >= 2) {
            throw new RuntimeException("logic");
        }
        bufCount += 1;
        if (bufCount == 1) {
            if (item1 != null) {
                throw new RuntimeException("logic");
            }
            item1 = new ItemsBuffer(bufcont);
        }
        else if (bufCount == 2) {
            if (item2 != null) {
                throw new RuntimeException("logic");
            }
            item2 = new ItemsBuffer(bufcont);
        }
    }

    public void add(int pos, int len, long ts, Ty ty) {
        if (bufCount == 1) {
            item1.add(pos, len, ts, ty);
        }
        else if (bufCount == 2) {
            item2.add(pos, len, ts, ty);
        }
        else {
            throw new RuntimeException("logic");
        }
    }

    public boolean notTerm() {
        return !term;
    }

    public synchronized void release() {
        if (item1 != null) {
            ItemsBuffer ib = item1;
            item1 = null;
            ib.release();
        }
        if (item2 != null) {
            ItemsBuffer ib = item2;
            item2 = null;
            ib.release();
        }
    }

    public void releaseFinite() {
        release();
    }

    public int getBufCount() {
        return bufCount;
    }

    public int tokenCount() {
        int s = 0;
        if (item1 != null) {
            s += item1.posL.size();
        }
        if (item2 != null) {
            s += item2.posL.size();
        }
        return s;
    }

    public List<BufCont> takeBuffers() {
        List<BufCont> a = new ArrayList<>();
        if (item1 != null) {
            a.add(item1.bufcont);
            item1.bufcont = null;
            item1 = null;
        }
        if (item2 != null) {
            a.add(item2.bufcont);
            item2.bufcont = null;
            item2 = null;
        }
        return a;
    }

    public static class FluxIterStats {
        public long openedCount;
        public long closedCount;
        public FluxIterStats() {
            openedCount = FluxIter.openedCount.get();
            closedCount = FluxIter.closedCount.get();
        }
    }

    static class FluxIter {
        static final AtomicLong openedCount = new AtomicLong();
        static final AtomicLong closedCount = new AtomicLong();
        int g1;
        ItemsBuffer ib;
        boolean done;
        boolean released;
        FluxIter(ItemsBuffer ib) {
            openedCount.getAndAdd(1);
            this.ib = ib;
        }
        synchronized void release() {
            closedCount.getAndAdd(1);
            ib.release();
            released = true;
        }
    }

    private static Flux<MapTsToken> fluxFromItem(int fid, ItemsBuffer ib, String partname) {
        if (ib == null) {
            return Flux.empty();
        }
        return Flux.<MapTsToken, FluxIter>generate(() -> new FluxIter(ib), (st, sink) -> {
            synchronized (st) {
                if (st.released) {
                    throw new RuntimeException("logic");
                }
                if (st.done) {
                    return st;
                }
                ItemsBuffer ib2 = st.ib;
                if (st.g1 < ib2.posL.size()) {
                    int i1 = st.g1;
                    // TODO provide origin
                    //String name = String.format("MapTsItemVec-fluxFromItem-%s-%d-of-%d", partname, i1, ib2.posL.size());
                    MapTsToken tok = new MapTsToken(ib2.bufcont.cloned(BufCont.Mark.ITEM_VEC_VGEN), fid, ib2.posL.get(i1), ib2.lenL.get(i1), ib2.tsL
                    .get(i1), ib2.tyL.get(i1));
                    st.g1 += 1;
                    sink.next(tok);
                }
                else {
                    st.done = true;
                    sink.complete();
                }
                return st;
            }
        }, FluxIter::release)
        .doFinally(k -> ib.release());
    }

    public Flux<MapTsToken> intoFlux(int fid) {
        Flux<MapTsToken> fl1 = fluxFromItem(fid, item1, "A");
        Flux<MapTsToken> fl2 = fluxFromItem(fid, item2, "B");
        item1 = null;
        item2 = null;
        return fl1.concatWith(fl2).doOnNext(kk -> kk.appendName(BufCont.Mark.ITEM_VEC_TFL));
    }

    public void markWith(BufCont.Mark prefix) {
        if (BufCont.doMark) {
            ItemsBuffer[] ibs = {item1, item2};
            for (int i1 = 0; i1 < ibs.length; i1 += 1) {
                ItemsBuffer ib = ibs[i1];
                if (ib != null) {
                    if (ib.bufcont != null) {
                        ib.bufcont.appendMark(prefix);
                        // TODO add the index here in some form
                    }
                }
            }
        }
    }

    // TODO remove
    public List<BufCont> testTakeBuffers() {
        if (item1 == null && item2 != null) {
            throw new RuntimeException("logic");
        }
        if (item1 != null) {
            BufCont bc1 = item1.bufcont;
            DataBuffer b1 = bc1.bufferRef();
            int n1 = item1.posL.size();
            if (n1 > 0) {
                b1.readPosition(0);
                b1.writePosition(item1.posL.get(n1-1) + item1.lenL.get(n1-1));
                b1.readPosition(item1.posL.get(0));
            }
            else {
                b1.readPosition(b1.writePosition());
            }
            //b1.readPosition(0);
            //b1.writePosition(b1.capacity());
            item1.bufcont = null;
            item1 = null;
            if (item2 != null) {
                BufCont bc2 = item2.bufcont;
                DataBuffer b2 = bc2.bufferRef();
                int n2 = item2.posL.size();
                if (n2 > 0) {
                    b2.readPosition(0);
                    b2.writePosition(item2.posL.get(n2-1) + item2.lenL.get(n2-1));
                    b2.readPosition(item2.posL.get(0));
                }
                else {
                    b2.readPosition(b2.writePosition());
                }
                //b2.readPosition(0);
                //b2.writePosition(b2.capacity());
                item2.bufcont = null;
                item2 = null;
                return List.of(bc1, bc2);
            }
            else {
                return List.of(bc1);
            }
        }
        else {
            return List.of();
        }
    }

}
