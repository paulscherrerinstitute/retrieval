package ch.psi.daq.retrieval.eventmap.ts;

import ch.psi.daq.retrieval.bytes.BufCont;
import ch.psi.daq.retrieval.merger.Releasable;
import ch.psi.daq.retrieval.merger.Markable;
import org.springframework.lang.NonNull;

public class MapTsToken implements Comparable<MapTsToken>, Markable, Releasable {
    public BufCont bufcont;
    public int fid;
    public int pos;
    public int len;
    public long ts;
    public MapTsItemVec.Ty ty;

    public MapTsToken(BufCont bufcont, int fid, int pos, int len, long ts, MapTsItemVec.Ty ty) {
        this.bufcont = bufcont;
        this.fid = fid;
        this.pos = pos;
        this.len = len;
        this.ts = ts;
        this.ty = ty;
    }

    public static int compare(MapTsToken a, MapTsToken b) {
        if (a.ts < b.ts) {
            return -1;
        }
        if (a.ts > b.ts) {
            return +1;
        }
        if (a.fid < b.fid) {
            return -1;
        }
        if (a.fid > b.fid) {
            return +1;
        }
        return 0;
    }

    @Override
    public int compareTo(@NonNull MapTsToken other) {
        return MapTsToken.compare(this, other);
    }

    public synchronized void release() {
        BufCont k = bufcont;
        bufcont = null;
        k.close();
    }

    @Override
    public void releaseFinite() {
        release();
    }

    public void appendName(BufCont.Mark mark) {
        if (BufCont.doMark) {
            if (bufcont != null) {
                bufcont.appendMark(mark);
            }
        }
    }

    @Override
    public void markWith(BufCont.Mark mark) {
        appendName(mark);
    }

}
