package ch.psi.daq.retrieval.merger;

import ch.psi.daq.retrieval.eventmap.ts.MapTsItemVec;
import ch.psi.daq.retrieval.eventmap.ts.MapTsToken;

import java.util.List;

public class CompleteEvent implements Releasable {

    public CompleteEvent(long ts, long pulse, List<MapTsToken> tokens) {
        this.ts = ts;
        this.pulse = pulse;
        this.tokens = tokens;
    }

    public boolean isValid() {
        if (tokens.size() < 1) {
            return false;
        }
        for (MapTsToken tok : tokens) {
            if (tok.ts != ts) {
                return false;
            }
            if (tok.pulse != pulse) {
                return false;
            }
        }
        MapTsItemVec.Ty ty0 = tokens.get(0).ty;
        if (ty0 == MapTsItemVec.Ty.FULL) {
            if (tokens.size() != 1) {
                return false;
            }
            return true;
        }
        else if (ty0 == MapTsItemVec.Ty.OPEN) {
            if (tokens.size() < 2) {
                return false;
            }
            for (int i1 = 1; i1 < tokens.size() - 1; i1 += 1) {
                MapTsItemVec.Ty ty = tokens.get(i1).ty;
                if (ty != MapTsItemVec.Ty.MIDDLE) {
                    return false;
                }
            }
            if (tokens.get(tokens.size() - 1).ty != MapTsItemVec.Ty.CLOSE) {
                return false;
            }
            return true;
        }
        else {
            return false;
        }
    }

    public List<MapTsToken> take() {
        List<MapTsToken> ret = tokens;
        ts = 0;
        pulse = 0;
        tokens = null;
        return ret;
    }

    public String toString() {
        if (tokens == null) {
            return "[empty]";
        }
        StringBuilder sb = new StringBuilder();
        for (MapTsToken tok : tokens) {
            sb.append(tok.ty.toString()).append("  ");
        }
        StringBuilder tss = new StringBuilder();
        for (MapTsToken tok : tokens) {
            tss.append(String.format("%d  ", tok.ts));
        }
        return String.format("CompleteEvent  n %d  ty F %s  L %s      %s    %s", tokens.size(), tokens.get(0).ty.toString(), tokens.get(tokens.size()-1).ty.toString(), sb.toString(), tss.toString());
    }

    public void releaseFinite() {
        ts = 0;
        pulse = 0;
        if (tokens != null) {
            for (MapTsToken tok : tokens) {
                tok.release();
            }
            tokens = List.of();
        }
    }

    public long ts;
    public long pulse;
    public List<MapTsToken> tokens;

}
