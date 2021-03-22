package ch.psi.daq.retrieval.merger;

import ch.psi.daq.retrieval.eventmap.ts.MapTsItemVec;

public interface MergeToken {

    MapTsItemVec.Ty ty();
    long ts();
    long pulse();

}
