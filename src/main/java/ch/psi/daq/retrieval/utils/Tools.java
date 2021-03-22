package ch.psi.daq.retrieval.utils;

import ch.psi.daq.retrieval.merger.Releasable;
import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;

public class Tools {
    static final Logger LOGGER = (Logger) LoggerFactory.getLogger(Tools.class.getSimpleName());

    public static void sleep(int ms) {
        long ts = System.nanoTime();
        long ns = 1000L * 1000 * ms;
        while (true) {
            long h = System.nanoTime() - ts;
            if (h >= ns) {
                break;
            }
            long g = ns - h;
            long k = 10;
            if (g < 1000L * 1000 * 10) {
                k = g / 1000 / 1000;
            }
            try {
                Thread.sleep(k);
            }
            catch (InterruptedException e) {
            }
        }
    }

    public static String formatTrace(Throwable e) {
        ByteArrayOutputStream ba = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(ba);
        ps.println(e.toString());
        Arrays.stream(e.getStackTrace()).limit(100).forEach(k -> ps.println(k.toString()));
        ps.close();
        return ba.toString(StandardCharsets.UTF_8);
    }

    public static <T extends Releasable> boolean passOrRelease(T k, Predicate<T> p) {
        if (p.test(k)) {
            return true;
        }
        else {
            k.releaseFinite();
            return false;
        }
    }

    public static <T> Flux<T> logSilence(Flux<T> fl, int timeout, String name) {
        if (true) {
            return fl;
        }
        // TODO
        AtomicLong tsLast = new AtomicLong(Long.MIN_VALUE);
        return fl.bufferTimeout(1, Duration.ofSeconds(timeout))
        .doOnNext(q -> {
            long now = System.nanoTime();
            if (q.size() == 0) {
                LOGGER.warn("TIMEOUT  {}  since {} s", name, (now - tsLast.get()) / 1000000000L);
            }
            else {
                tsLast.set(now);
            }
        })
        .concatMapIterable(Function.identity());

    }

}
