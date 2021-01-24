package ch.psi.daq.retrieval.throttle;

import org.springframework.core.io.buffer.DataBuffer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;

public class Throttle {

    public static Flux<DataBuffer> throttle(Flux<DataBuffer> fl, long rate, int steps, int interval, int overslack) {
        if (rate > 0) {
            Throttle throttle = new Throttle(rate, steps, interval, overslack);
            return fl.concatMap(throttle::map, 0);
        }
        else {
            return fl;
        }
    }

    public static Flux<ByteBuffer> throttleByteBuffer(Flux<ByteBuffer> fl, long rate, int steps, int interval, int overslack) {
        if (rate > 0) {
            Throttle throttle = new Throttle(rate, steps, interval, overslack);
            return fl.concatMap(throttle::map, 0);
        }
        else {
            return fl;
        }
    }

    Throttle(long rate, int steps, int interval, int overslack) {
        this.rate = rate;
        this.interval = (interval > 0 ? interval : 400) * 1000 * 1000;
        this.overslack = (overslack > 0 ? overslack : 100);
        acc = new long[steps > 0 ? steps : 16];
        chunk = (this.interval / 1000) * (this.rate / 1000) / 1000;
    }

    Mono<DataBuffer> map(DataBuffer buf) {
        return map2(buf, buf.readableByteCount());
    }

    Mono<ByteBuffer> map(ByteBuffer buf) {
        return map2(buf, buf.remaining());
    }

    <T> Mono<T> map2(T obj, int len) {
        long tsNow = System.nanoTime();
        if (fresh) {
            fresh = false;
            tsBeg = tsNow;
            Arrays.fill(acc, chunk);
            ix = 0;
            acc[ix] = 0;
        }
        long j = (tsNow - tsBeg) / interval;
        tsBeg = tsBeg + j * interval;
        if (j > acc.length) {
            Arrays.fill(acc, chunk);
            ix = 0;
            acc[ix] = 0;
        }
        else {
            for (int i = 0; i < j; i += 1) {
                ix = (ix + 1) % acc.length;
                acc[ix] = 0;
            }
        }
        acc[ix] += len;
        long tot = tot();
        long dtC = (long) (acc.length - 1) * interval;
        long tsMin = tsBeg - dtC;
        long tsE = tsMin + tot * 100000 / rate * 10000;
        if (tsE > tsNow) {
            long del = tsE - tsNow;
            return Mono.just(obj).delayElement(Duration.ofMillis(del / 1000 / 1000));
        }
        else {
            return Mono.just(obj);
        }
    }

    long tot() {
        long s = 0;
        for (long k : acc) {
            s += k;
        }
        return s;
    }

    int ix;
    boolean fresh = true;
    long tsBeg;
    long[] acc;
    long rate;
    int interval;
    int overslack;
    long chunk;

}
