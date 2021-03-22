package ch.psi.daq.retrieval.eventmap.basic;

import ch.psi.daq.retrieval.bytes.BufCont;
import ch.psi.daq.retrieval.merger.Releasable;

import java.util.ArrayList;
import java.util.List;

public class BasicResult implements Releasable {

    public static BasicResult create(int cap) {
        BasicResult ret = new BasicResult();
        ret.chunks = new ArrayList<>(cap);
        return ret;
    }

    public static BasicResult term() {
        BasicResult ret = new BasicResult();
        ret.term = true;
        return ret;
    }

    public boolean isTerm() {
        return term;
    }

    public boolean notTerm() {
        return !term;
    }

    public void bufBegin(BufCont bufCont) {
        bufCount += 1;
        if (bufCount < 0 || bufCount > 1) {
            throw new RuntimeException("logic");
        }
        bufs[bufCount] = bufCont.cloned(BufCont.Mark.MapBasicBufBegin);
    }

    public void add(Chunk chunk) {
        chunk.setBufIx(bufCount);
        chunks.add(chunk);
    }

    public void releaseFinite() {
        release();
    }

    public void release() {
        if (chunks != null) {
            chunks.clear();
        }
        for (int i = 0; i < bufs.length; i += 1) {
            if (bufs[i] != null) {
                bufs[i].close();
                bufs[i] = null;
            }
        }
    }

    public List<Chunk> chunks;
    public BufCont[] bufs = new BufCont[2];
    int bufCount = -1;
    boolean term;

}
