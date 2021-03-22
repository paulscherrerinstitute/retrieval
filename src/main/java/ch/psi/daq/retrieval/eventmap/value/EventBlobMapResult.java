package ch.psi.daq.retrieval.eventmap.value;

import ch.psi.daq.retrieval.bytes.BufCont;
import ch.psi.daq.retrieval.merger.Releasable;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class EventBlobMapResult implements Releasable {
    static final AtomicLong takenEmpty = new AtomicLong();
    List<BufCont> bufconts;
    boolean term;

    public static EventBlobMapResult term() {
        EventBlobMapResult ret = new EventBlobMapResult();
        ret.term = true;
        return ret;
    }

    public static EventBlobMapResult empty() {
        EventBlobMapResult ret = new EventBlobMapResult();
        return ret;
    }

    public static EventBlobMapResult fromBuffers(List<BufCont> bufconts) {
        EventBlobMapResult ret = new EventBlobMapResult();
        ret.bufconts = bufconts;
        return ret;
    }

    private EventBlobMapResult() {}

    public boolean hasContent() {
        return bufconts != null;
    }

    public boolean notTerm() {
        return !term;
    }

    public List<BufCont> takeBufCont() {
        if (bufconts == null) {
            takenEmpty.getAndAdd(1);
            return List.of();
        }
        else {
            List<BufCont> ret = bufconts;
            bufconts = null;
            ret.forEach(k -> k.appendMark(BufCont.Mark.MapResultTaken));
            return ret;
        }
    }

    public void release() {
        if (bufconts != null) {
            List<BufCont> l = bufconts;
            bufconts = null;
            l.forEach(BufCont::close);
        }
    }

    public void releaseFinite() {
        release();
    }

    public void appendMark(BufCont.Mark mark) {
        if (bufconts != null) {
            bufconts.forEach(k -> k.appendMark(mark));
        }
    }

}
