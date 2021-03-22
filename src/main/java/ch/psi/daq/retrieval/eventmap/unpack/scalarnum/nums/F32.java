package ch.psi.daq.retrieval.eventmap.unpack.scalarnum.nums;

import ch.psi.daq.retrieval.eventmap.unpack.scalarnum.Acc;
import ch.psi.daq.retrieval.eventmap.unpack.scalarnum.Avg;
import ch.psi.daq.retrieval.eventmap.unpack.scalarnum.Num;

public class F32 implements Num<F32> {

    public Acc<F32> createAcc() {
        return new AccF32();
    }

    public Avg<F32> createMinMaxAvg() {
        return new AvgF32<>(new F32());
    }

}
