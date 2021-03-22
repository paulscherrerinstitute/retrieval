package ch.psi.daq.retrieval.eventmap.unpack.scalarnum;

import ch.psi.daq.retrieval.eventmap.agg.Agg;

public interface IntAgg<T extends Num<T>>  extends Agg {

    void consider(UnpackScalarNumRes<T> req, int p1, int p2);

    String finalString();

}
