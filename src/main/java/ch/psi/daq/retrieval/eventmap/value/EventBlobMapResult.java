package ch.psi.daq.retrieval.eventmap.value;

import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;

public class EventBlobMapResult {
    public DataBuffer buf;
    public boolean term;

    public void release() {
        if (buf != null) {
            DataBufferUtils.release(buf);
            buf = null;
        }
    }

}
