package ch.psi.daq.retrieval.eventmap.unpack.scalarnum;

import org.springframework.core.io.buffer.DataBuffer;

import java.util.ArrayList;
import java.util.List;

public class UnpackScalarNumRes<T extends Num<T>> {

    public UnpackScalarNumRes(Num<T> num) {
        acc = num.createAcc();
    }

    public List<Long> tss() {
        return tss;
    }

    public List<Long> pulses() {
        return pulses;
    }

    public Acc<T> acc() {
        return acc;
    }

    public void consume(long ts, long pulse, DataBuffer buf) {
        tss.add(ts);
        pulses.add(pulse);
        acc.consume(buf);
    }

    @Override
    public String toString() {
        return String.format("UnpackScalarNumRes<>  with acc %s", acc);
    }

    List<Long> tss = new ArrayList<>();
    List<Long> pulses = new ArrayList<>();
    Acc<T> acc;

}
