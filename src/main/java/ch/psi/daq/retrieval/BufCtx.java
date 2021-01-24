package ch.psi.daq.retrieval;

import org.springframework.core.io.buffer.DataBufferFactory;

public class BufCtx {
    public DataBufferFactory bufFac;
    public int bufferSize;
    public BufCtx(DataBufferFactory bufFac, int bufferSize) {
        this.bufFac = bufFac;
        this.bufferSize = bufferSize;
    }
}
