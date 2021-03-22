package ch.psi.daq.retrieval.eventmap.unpack.scalarnum.nums;

import ch.psi.daq.retrieval.eventmap.unpack.scalarnum.Acc;
import org.springframework.core.io.buffer.DataBuffer;

import java.util.ArrayList;
import java.util.List;

public class AccI64 implements Acc<I64> {

    public void consume(DataBuffer buf) {
        if (buf.readableByteCount() != 8) {
            throw new RuntimeException("unexpected length");
        }
        else {
            // TODO pass the extract handler depending on endianness via ctor
            vals.add(buf.asByteBuffer().getLong());
        }
    }

    @Override
    public String toString() {
        return String.format("AccI64  %d  %s", vals.size(), vals);
    }

    final List<Long> vals = new ArrayList<>();

}
