package ch.psi.daq.retrieval.merger;

import ch.psi.daq.retrieval.bytes.BufCont;
import ch.qos.logback.classic.Logger;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

class SubOut<T extends Comparable<T> & Markable & Releasable> implements Subscription {
    static final Logger LOGGER = (Logger) LoggerFactory.getLogger(SubOut.class.getSimpleName());
    AtomicInteger cancelled = new AtomicInteger();
    AtomicLong requested = new AtomicLong();
    Subscriber<? super T> sub;
    Merger<T> pub;
    AtomicInteger inDet = new AtomicInteger();
    AtomicInteger error = new AtomicInteger();

    SubOut(Merger<T> pub, Subscriber<? super T> sub) {
        this.pub = pub;
        this.sub = sub;
    }

    @Override
    public void cancel() {
        if (cancelled.compareAndExchange(0, 1) == 0) {
            pub.downstreamCancelled();
        }
        else {
            LOGGER.error("cancel  WAS ALREADY BEFORE");
        }
    }

    @Override
    public void request(long n) {
        if (inDet.compareAndExchange(0, 1) != 0) {
            error.set(1);
            sub.onError(new RuntimeException("violation"));
            return;
        }
        request2(n);
        if (inDet.compareAndExchange(1, 0) != 1) {
            error.set(1);
            sub.onError(new RuntimeException("violation"));
        }
    }

    void request2(long n) {
        if (cancelled.get() != 0) {
            return;
        }
        if (n <= 0) {
            sub.onError(new IllegalArgumentException("request n <= 0"));
        }
        else {
            try {
                Math.addExact(requested.get(), n);
                requested.getAndAdd(n);
            }
            catch (ArithmeticException e) {
                requested.set(Long.MAX_VALUE);
            }
            pub.produce();
        }
    }

    void signalNext(T t) {
        t.markWith(BufCont.Mark.SUB_OUT_NEXT);
        sub.onNext(t);
    }

    void signalComplete() {
        if (cancelled.getAndSet(1) == 0) {
            sub.onComplete();
        }
        else {
            LOGGER.info("signalComplete  BUT CANCELLED");
        }
    }

    void signalError(Throwable e) {
        if (cancelled.getAndSet(1) == 0) {
            sub.onError(e);
        }
        else {
            LOGGER.info("signalError  BUT CANCELLED");
        }
    }

}
