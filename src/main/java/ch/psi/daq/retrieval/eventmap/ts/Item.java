package ch.psi.daq.retrieval.eventmap.ts;

import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.*;

import java.util.List;

public class Item {
    static Logger LOGGER = (Logger) LoggerFactory.getLogger("MapTsItem");
    public ItemP item1;
    public ItemP item2;
    public boolean term;
    public int ix;
    public boolean isLast;

    public static Item dummy(DataBufferFactory bufFac) {
        Item ret = new Item();
        ret.item1 = new ItemP();
        ret.item1.buf = bufFac.wrap(new byte[] { (byte) 0x92 });
        return ret;
    }

    @Override
    public String toString() {
        return String.format("TsItem { %7s  ix: %d  term: %s  isPlain %s  item1: %s, item2: %s }", shortDesc(), ix, term, isPlainBuffer(), item1, item2);
    }

    public boolean isTerm() {
        return term;
    }

    public boolean verify() {
        if (isPlainBuffer() && item1.buf == null) {
            LOGGER.error("plain buffer without buf: {}", this);
            return false;
        }
        if (term) {
            if (item1 != null) {
                LOGGER.error("has item1 despite term");
                return false;
            }
            if (item2 != null) {
                LOGGER.error("has item2 despite term  {}", this);
                return false;
            }
        }
        else {
            if (item1 == null || item1.buf == null) {
                LOGGER.error("item1 == null || item1.buf == null  {}", this);
                return false;
            }
            if (item2 != null && item2.buf == null) {
                LOGGER.error("item2 == null || item2.buf == null  {}", this);
                return false;
            }
        }
        return true;
    }

    public boolean hasMoreMarkers() {
        return item1 != null && ix < item1.c;
    }

    public boolean isPlainBuffer() {
        return !term && item1 != null && item1.c == 0 && item1.p1 != item1.p2;
    }

    public String shortDesc() {
        if (isPlainBuffer()) {
            return "PLAIN";
        }
        if (!hasMoreMarkers()) {
            return "NOMORE";
        }
        if (item1.ty[ix] == 1) {
            return "BEGIN";
        }
        if (item1.ty[ix] == 2) {
            return "END";
        }
        return "UNKNOWN";
    }

    public void adv() {
        if (isTerm()) {
            LOGGER.warn("adv called despite isTerm");
        }
        if (item1 == null) {
            if (item2 != null) {
                LOGGER.error("\n\nPANIC  logic error 88371\n\n");
                release();
                item2 = null;
            }
            return;
        }
        ix += 1;
        if (ix >= item1.c) {
            ix = 0;
            item1.release();
            if (item2 != null) {
                item1 = item2;
                item2 = null;
            }
            else {
                item1 = null;
            }
        }
    }

    public void release() {
        if (item1 != null) {
            item1.release();
            item1 = null;
        }
        if (item2 != null) {
            item2.release();
            item2 = null;
        }
    }

    public long totalReadable() {
        long n = 0;
        if (item1 != null && item1.buf != null) {
            n += item1.buf.readableByteCount();
        }
        if (item2 != null && item2.buf != null) {
            n += item2.buf.readableByteCount();
        }
        return n;
    }

    public List<DataBuffer> takeBuffers() {
        List<DataBuffer> ret;
        if (item1 != null && item1.buf != null) {
            if (item2 != null && item2.buf != null) {
                ret = List.of(item1.buf, item2.buf);
                item1.buf = null;
                item2.buf = null;
            }
            else {
                ret = List.of(item1.buf);
                item1.buf = null;
            }
        }
        else if (item2 != null && item2.buf != null) {
            DataBufferUtils.release(item2.buf);
            item2.buf = null;
            LOGGER.error("logic");
            throw new RuntimeException("logic");
        }
        else {
            LOGGER.warn("Warning: empty item");
            ret = List.of();
        }
        return ret;
    }

}
