package ch.psi.daq.retrieval.eventmap.value;

import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DataBufferUtils;

import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public class OutputBuffer extends OutputStream {
    DataBufferFactory bufFac;
    DataBuffer buf;
    int bufferSize = 8 * 1024;
    List<DataBuffer> pendingBuffers = new ArrayList<>();
    long totalPending;

    public OutputBuffer(DataBufferFactory bufFac) {
        this.bufFac = bufFac;
    }

    public OutputBuffer(DataBufferFactory bufFac, int bufferSize) {
        this.bufFac = bufFac;
        this.bufferSize = bufferSize;
    }

    @Override
    public void write(int val) {
        if (buf == null) {
            buf = bufFac.allocateBuffer(bufferSize);
        }
        if (buf.writableByteCount() < 1) {
            pendingBuffers.add(buf);
            buf = bufFac.allocateBuffer(bufferSize);
        }
        buf.write((byte) val);
        totalPending += 1;
    }

    @Override
    public void write(byte[] ba, int off, int len) {
        if (buf == null) {
            buf = bufFac.allocateBuffer(bufferSize);
        }
        int wc = buf.writableByteCount();
        if (wc >= len) {
            buf.write(ba, off, len);
            totalPending += len;
        }
        else {
            pendingBuffers.add(buf);
            buf = bufFac.allocateBuffer(bufferSize);
            wc = buf.writableByteCount();
            if (wc >= len) {
                buf.write(ba, off, len);
                totalPending += len;
            }
            else {
                throw new RuntimeException("Large writes not supported so far");
            }
        }
    }

    @Override
    public void close() {
        pendingBuffers.add(buf);
        buf = null;
    }

    public void release() {
        if (buf != null) {
            DataBufferUtils.release(buf);
            buf = null;
        }
        if (pendingBuffers != null) {
            for (DataBuffer buf : pendingBuffers) {
                DataBufferUtils.release(buf);
            }
            pendingBuffers = null;
        }
    }

    public List<DataBuffer> getPending() {
        for (DataBuffer b : pendingBuffers) {
            totalPending -= b.readableByteCount();
        }
        if (totalPending < 0) {
            throw new RuntimeException("inconsistent totalPending");
        }
        List<DataBuffer> ret = pendingBuffers;
        pendingBuffers = new ArrayList<>();
        return ret;
    }

    public long totalPending() {
        return totalPending;
    }

}
