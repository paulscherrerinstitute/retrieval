package ch.psi.daq.retrieval.bytes;

import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;

import java.util.ArrayList;
import java.util.List;

public class Output {
    static final Logger LOGGER = (Logger) LoggerFactory.getLogger(Output.class.getSimpleName());

    public Output(DataBufferFactory bufFac, int bufferSize, BufCont.Mark mark) {
        this.bufFac = bufFac;
        this.bufferSize = bufferSize;
        this.mark = mark;
        startNew();
    }

    public DataBuffer bufferRef() {
        return cbufcont.bufferRef();
    }

    void startNew() {
        if (cbufcont != null) {
            cbufcont.appendMark(BufCont.Mark.OutAdd);
            bufs.add(cbufcont);
            int n = bufs.size();
            if (n == 20 || n == 40 || n == 80 || n == 160) {
                LOGGER.info("lots of buffers {}", bufs.size());
            }
            cbufcont = null;
        }
        cbufcont = BufCont.allocate(bufFac, bufferSize, BufCont.Mark.OutNew);
        cbufcont.appendMark(mark);
    }

    public void ensureWritable(int n) {
        if (cbufcont == null) {
            startNew();
        }
        else {
            if (cbufcont.bufferRef().writableByteCount() < n) {
                startNew();
            }
            if (cbufcont.bufferRef().writableByteCount() < n) {
                throw new RuntimeException("logic");
            }
        }
    }

    public void transfer(DataBuffer buf, int n) {
        DataBuffer cbuf = cbufcont.bufferRef();
        int z = n;
        while (z > 0) {
            if (cbuf.writableByteCount() <= 0) {
                cbuf = null;
                startNew();
                cbuf = cbufcont.bufferRef();
            }
            if (cbuf.writableByteCount() <= 0) {
                throw new RuntimeException("logic");
            }
            int h = Math.min(z, cbuf.writableByteCount());
            cbuf.write(buf.slice(buf.readPosition(), h));
            buf.readPosition(buf.readPosition() + h);
            z -= h;
        }
    }

    public List<BufCont> take() {
        if (cbufcont != null) {
            if (cbufcont.readableByteCount() > 0) {
                bufs.add(cbufcont);
                cbufcont = null;
            }
            else {
                BufCont b = cbufcont;
                cbufcont = null;
                b.close();
            }
        }
        List<BufCont> ret = bufs;
        bufs = new ArrayList<>();
        return ret;
    }

    public void release() {
        take().forEach(BufCont::close);
    }

    public int queuedCount() {
        return bufs.size();
    }

    final BufCont.Mark mark;
    final DataBufferFactory bufFac;
    final int bufferSize;
    BufCont cbufcont;
    List<BufCont> bufs = new ArrayList<>();

}
