package ch.psi.daq.retrieval.reqctx;

import org.springframework.core.io.buffer.DataBufferFactory;

public class BufCtx {

    public BufCtx(DataBufferFactory bufFac) {
        this.bufFac = bufFac;
        this.bufferSize = 1024 * 32;
    }

    public final DataBufferFactory bufFac;
    public final int bufferSize;

}
