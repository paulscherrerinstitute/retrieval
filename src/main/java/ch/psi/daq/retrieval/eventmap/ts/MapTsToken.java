package ch.psi.daq.retrieval.eventmap.ts;

import ch.psi.daq.retrieval.bytes.BufCont;
import ch.psi.daq.retrieval.merger.MergeToken;
import ch.psi.daq.retrieval.merger.Releasable;
import ch.psi.daq.retrieval.merger.Markable;
import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.NonNull;

public class MapTsToken implements Comparable<MapTsToken>, Markable, Releasable, MergeToken {
    static final Logger LOGGER = (Logger) LoggerFactory.getLogger(MapTsToken.class.getSimpleName());

    public MapTsToken(BufCont bufcont, int fid, int pos, int len, long ts, long pulse, MapTsItemVec.Ty ty) {
        this.bufcont = bufcont;
        this.fid = fid;
        this.pos = pos;
        this.len = len;
        this.ts = ts;
        this.pulse = pulse;
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
        if (k != null) {
            k.close();
        }
        else if (!released) {
            LOGGER.warn("not released, bufcont null");
        }
        released = true;
    }

    @Override
    public void releaseFinite() {
        release();
    }

    public void appendMark(BufCont.Mark mark) {
        if (BufCont.doMark) {
            if (bufcont != null) {
                bufcont.appendMark(mark);
            }
        }
    }

    @Override
    public void markWith(BufCont.Mark mark) {
        appendMark(mark);
    }

    @Override
    public MapTsItemVec.Ty ty() {
        return ty;
    }

    @Override
    public long ts() {
        return ts;
    }

    @Override
    public long pulse() {
        return pulse;
    }

    public BufCont bufcont;
    public int fid;
    public int pos;
    public int len;
    public long ts;
    public long pulse;
    public MapTsItemVec.Ty ty;
    boolean released;

}
