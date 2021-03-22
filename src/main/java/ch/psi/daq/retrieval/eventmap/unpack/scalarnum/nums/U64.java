package ch.psi.daq.retrieval.eventmap.unpack.scalarnum.nums;

import ch.psi.daq.retrieval.eventmap.unpack.scalarnum.Acc;
import ch.psi.daq.retrieval.eventmap.unpack.scalarnum.Avg;
import ch.psi.daq.retrieval.eventmap.unpack.scalarnum.Num;

public class U64 implements Num<U64> {

    public Acc<U64> createAcc() {
        return new AccU64();
    }

    public Avg<U64> createMinMaxAvg() {
        return new AvgU64<>(new U64());
    }

}
