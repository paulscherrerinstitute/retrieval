package ch.psi.daq.retrieval.subnodes;

import ch.psi.daq.retrieval.config.Node;
import ch.psi.daq.retrieval.controller.Parts;
import ch.psi.daq.retrieval.merger.Releasable;

class SubresInter implements Releasable {

    @Override
    public void releaseFinite() {
        parts.release();
        subHeadRes.releaseFinite();
    }

    Node node;
    SubStream subStream;
    Parts parts;
    SubHeadRes subHeadRes;

}
