package ch.psi.daq.retrieval.eventmap.unpack.scalarnum.nums;

import ch.psi.daq.retrieval.eventmap.unpack.scalarnum.Acc;
import org.springframework.core.io.buffer.DataBuffer;

import java.util.ArrayList;
import java.util.List;

public class AccF32 implements Acc<F32> {

    public void consume(DataBuffer buf) {
        if (buf.readableByteCount() != 4) {
            throw new RuntimeException("unexpected length");
        }
        else {
            // TODO pass the extract handler depending on endianness via ctor
            vals.add(buf.asByteBuffer().getFloat());
        }
    }

    @Override
    public String toString() {
        return String.format("AccU64  %d  %s", vals.size(), vals);
    }

    final List<Float> vals = new ArrayList<>();

}
