package ch.psi.daq.retrieval.eventmap.unpack.scalarnum;

public interface Avg<T extends Num<T>> extends IntAgg<T> {

    @Override
    default String name() {
        return "minmaxavg";
    }

}
