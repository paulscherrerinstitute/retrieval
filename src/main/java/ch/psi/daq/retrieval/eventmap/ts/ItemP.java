package ch.psi.daq.retrieval.eventmap.ts;

import com.google.common.io.BaseEncoding;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;

import java.util.Arrays;

public class ItemP {
    public int c;
    public long[] ts;
    public int[] pos;
    public int[] ty;
    public int[] len;
    public DataBuffer buf;
    public int p1;
    public int p2;

    String format(DataBuffer buf) {
        if (true) return "[.]";
        byte[] a = new byte[Math.min(16, buf.writePosition())];
        buf.slice(0, buf.capacity()).read(a);
        return BaseEncoding.base16().lowerCase().encode(a);
    }

    @Override
    public String toString() {
        return String.format("ItemP {\n  p1: %5d  p2: %5d  c: %3d\n  ts: %s\n  ty: %s  pos: %s\n  buf: %s\n}", p1, p2, c,
        ts == null ? null : Arrays.toString(Arrays.copyOf(ts, c)),
        ty == null ? null : Arrays.toString(Arrays.copyOf(ty, c)),
        pos == null ? null : Arrays.toString(Arrays.copyOf(pos, c)),
        buf == null ? "null" : format(buf)
        );
    }

    @Override
    public boolean equals(Object x2) {
        if (!(x2 instanceof ItemP)) return false;
        ItemP x = (ItemP) x2;
        return c == x.c &&
        Arrays.equals(ts, 0, c, x.ts, 0, x.c) &&
        Arrays.equals(pos, 0, c, x.pos, 0, x.c) &&
        Arrays.equals(ty, 0, c, x.ty, 0, x.c);
    }

    public void release() {
        if (buf != null) {
            DataBufferUtils.release(buf);
            buf = null;
        }
    }

}
