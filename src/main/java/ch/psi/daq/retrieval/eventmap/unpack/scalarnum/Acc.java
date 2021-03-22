package ch.psi.daq.retrieval.eventmap.unpack.scalarnum;

import org.springframework.core.io.buffer.DataBuffer;

public interface Acc<T extends Num<T>> {

    void consume(DataBuffer buf);

}
