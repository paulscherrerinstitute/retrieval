package ch.psi.daq.retrieval.eventmap.unpack.scalarnum;

public interface Num<T extends Num<T>> {

    Acc<T> createAcc();
    Avg<T> createMinMaxAvg();

}
