package ch.psi.daq.retrieval.eventmap.unpack;

import ch.psi.daq.retrieval.merger.Releasable;

public class UnpackResult implements Releasable {

    public static UnpackResult create() {
        return new UnpackResult();
    }

    public boolean notTerm() {
        return !term;
    }

    @Override
    public void releaseFinite() {
    }

    boolean term;

}
