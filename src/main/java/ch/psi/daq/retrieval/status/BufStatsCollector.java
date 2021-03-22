package ch.psi.daq.retrieval.status;

import ch.psi.daq.retrieval.bytes.BufCont;
import org.springframework.core.io.buffer.DataBuffer;
import reactor.core.publisher.Flux;

public class BufStatsCollector {

    public static BufStatsCollector create(String name) {
        BufStatsCollector ret = new BufStatsCollector(name);
        return ret;
    }

    BufStatsCollector(String name) {
        this.name = name;
    }

    public Flux<BufCont> apply(Flux<BufCont> fl) {
        return fl.doOnNext(k -> {
            if (k.hasBuf()) {
                DataBuffer buf = k.bufferRef();
                capSum += buf.capacity();
                rbSum += buf.readableByteCount();
                if (count == 0) {
                    capMin = buf.capacity();
                    capMax = buf.capacity();
                    rbMin = buf.readableByteCount();
                    rbMax = buf.readableByteCount();
                    tsLast = System.nanoTime();
                }
                else {
                    capMin = Math.min(buf.capacity(), capMin);
                    capMax = Math.max(buf.capacity(), capMax);
                    rbMin = Math.min(buf.readableByteCount(), rbMin);
                    rbMax = Math.max(buf.readableByteCount(), rbMax);
                    long dt = System.nanoTime() - tsLast;
                    if (count == 1) {
                        minDt = dt;
                        maxDt = dt;
                    }
                    else {
                        minDt = Math.min(dt, minDt);
                        maxDt = Math.max(dt, maxDt);
                    }
                }
            }
            else {
                nobuf += 1;
            }
            count += 1;
        });
    }

    public String name;
    public long count;
    public int nobuf;
    public int capMin;
    public int capMax;
    public long capSum;
    public int rbMin;
    public int rbMax;
    public long rbSum;
    long tsLast;
    public long minDt;
    public long maxDt;

}
