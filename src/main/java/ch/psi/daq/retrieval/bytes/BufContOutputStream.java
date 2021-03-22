package ch.psi.daq.retrieval.bytes;

import ch.psi.daq.retrieval.reqctx.BufCtx;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.lang.NonNull;

import java.io.OutputStream;
import java.util.List;

public class BufContOutputStream extends OutputStream {

    public BufContOutputStream(BufCtx bufCtx, BufCont.Mark mark) {
        this.bufCtx = bufCtx;
        this.mark = mark;
        output = new Output(bufCtx.bufFac, bufCtx.bufferSize, mark);
    }

    @Override
    public void write(int val) {
        output.ensureWritable(1);
        output.bufferRef().write((byte) val);
    }

    @Override
    public void write(@NonNull byte[] ba, int off, int len) {
        DataBuffer buf;
        buf = output.bufferRef();
        int wb = buf.writableByteCount();
        while (len > 0) {
            if (wb > 0) {
                int n = Math.min(wb, len);
                buf.write(ba, off, n);
                off += n;
                len -= n;
            }
            if (len <= 0) {
                return;
            }
            output.ensureWritable(1);
        }
    }

    @Override
    public void close() {
    }

    public void release() {
        output.release();
    }

    public int queuedCount() {
        return output.queuedCount();
    }

    public List<BufCont> takeAndStart() {
        List<BufCont> q = output.take();
        output.startNew();
        return q;
    }

    public List<BufCont> take() {
        return output.take();
    }

    Output output;
    BufCtx bufCtx;
    BufCont.Mark mark;

}
