package ch.psi.daq.retrieval.eventmap.unpack.scalarnum.nums;

import ch.psi.daq.retrieval.eventmap.unpack.scalarnum.Acc;
import ch.psi.daq.retrieval.eventmap.unpack.scalarnum.Avg;
import ch.psi.daq.retrieval.eventmap.unpack.scalarnum.Num;

public class I64 implements Num<I64> {

    public Acc<I64> createAcc() {
        return new AccI64();
    }

    public Avg<I64> createMinMaxAvg() {
        return new AvgI64<>(new I64());
    }

}
