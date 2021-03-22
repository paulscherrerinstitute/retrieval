package ch.psi.daq.retrieval.utils;

import ch.qos.logback.classic.Logger;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import ch.psi.daq.retrieval.utils.Tools;

public class PubRepeat<T> implements Publisher<T>, Subscriber<T> {
    static final Logger LOGGER = (Logger) LoggerFactory.getLogger(PubRepeat.class.getSimpleName());

    public PubRepeat(Publisher<T> pub, String name) {
        this.pub = pub;
        this.timeout = 0;
        this.name = name;
    }

    public PubRepeat(Publisher<T> pub, int timeout, String name) {
        this.pub = pub;
        this.timeout = timeout;
        this.name = name;
    }

    @Override
    public synchronized void subscribe(Subscriber<? super T> sub) {
        this.sub = sub;
        pub.subscribe(sub);
    }

    @Override
    public synchronized void onSubscribe(Subscription scr) {
        this.scr = scr;
        if (timeout > 0) {
            this.wd = new Thread(this::watchdog);
            this.wd.start();
        }
        sub.onSubscribe(scr);
    }

    @Override
    public synchronized void onNext(T t) {
        nextLast.set(System.nanoTime());
        sub.onNext(t);
    }

    @Override
    public synchronized void onError(Throwable t) {
        term.set(1);
        sub.onError(t);
    }

    @Override
    public synchronized void onComplete() {
        term.set(1);
        sub.onComplete();
    }

    synchronized void request(long n) {
        requestedLast.set(System.nanoTime());
        scr.request(n);
    }

    synchronized void cancel() {
        term.set(1);
        scr.cancel();
    }

    void watchdog() {
        wdRun.getAndAdd(1);
        while (term.get() == 0) {
            Tools.sleep(1000);
            long r = requested.get();
            if (r > 0) {
                long dt = System.nanoTime() - nextLast.get();
                dt /= 1000000;
                if (dt > timeout) {
                    timeout = timeout * 4 / 3;
                    timeoutCount.getAndAdd(1);
                    LOGGER.warn("{}  req {}  dt {}", name, r, dt);
                }
            }
            else {
                long dt = System.nanoTime() - requestedLast.get();
                dt /= 1000000;
                if (dt > 8000) {
                    nothingRequestedCount.getAndAdd(1);
                }
            }
        }
        wdRun.getAndAdd(-1);
    }

    public static class Stats {
        public long timeoutCount;
        public long nothingRequestedCount;
        public long wdRun;
        public Stats() {
            timeoutCount = PubRepeat.timeoutCount.get();
            nothingRequestedCount = PubRepeat.nothingRequestedCount.get();
            wdRun = PubRepeat.wdRun.get();
        }
    }

    static final AtomicLong timeoutCount = new AtomicLong();
    static final AtomicLong nothingRequestedCount = new AtomicLong();
    static final AtomicLong wdRun = new AtomicLong();
    final String name;
    final Publisher<T> pub;
    final AtomicInteger term = new AtomicInteger();
    final AtomicLong requested = new AtomicLong();
    final AtomicLong nextLast = new AtomicLong();
    final AtomicLong requestedLast = new AtomicLong();
    Thread wd;
    int timeout;
    Subscriber<? super T> sub;
    Subscription scr;

}
