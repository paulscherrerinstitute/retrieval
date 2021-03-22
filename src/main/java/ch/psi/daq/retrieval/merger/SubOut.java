package ch.psi.daq.retrieval.merger;

import ch.psi.daq.retrieval.bytes.BufCont;
import ch.psi.daq.retrieval.eventmap.ts.MapTsItemVec;
import ch.psi.daq.retrieval.eventmap.ts.MapTsToken;
import ch.qos.logback.classic.Logger;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

class SubOut<T extends Comparable<T> & Markable & Releasable & MergeToken> implements Subscription {
    static final Logger LOGGER = (Logger) LoggerFactory.getLogger(SubOut.class.getSimpleName());
    final AtomicInteger cancelled = new AtomicInteger();
    final AtomicLong requested = new AtomicLong();
    final Subscriber<? super T> sub;
    final Merger<T> pub;
    final AtomicInteger inDet = new AtomicInteger();
    final AtomicInteger error = new AtomicInteger();
    MapTsItemVec.Ty tyLast;

    SubOut(Merger<T> pub, Subscriber<? super T> sub) {
        this.pub = pub;
        this.sub = sub;
    }

    @Override
    public void cancel() {
        if (cancelled.compareAndExchange(0, 1) == 0) {
            pub.downstreamCancel();
        }
        else {
            LOGGER.error("cancel  already cancelled before");
        }
    }

    @Override
    public void request(long n) {
        if (cancelled.get() != 0) {
            return;
        }
        if (n <= 0) {
            sub.onError(new IllegalArgumentException("request n <= 0"));
            return;
        }
        request2(n);
    }

    void request2(long n) {
        if (n < Long.MAX_VALUE - requested.get()) {
            requested.getAndAdd(n);
        }
        else {
            requested.set(Long.MAX_VALUE);
        }
        if (inDet.compareAndExchange(0, 1) == 0) {
            pub.produce();
            if (inDet.compareAndExchange(1, 0) != 1) {
                error.set(1);
                sub.onError(new RuntimeException("logic"));
            }
        }
    }

    void signalNext(T t) {
        MapTsItemVec.Ty ty = t.ty();
        long pulse = t.pulse();
        if (tyLast != null) {
            if (ty == MapTsItemVec.Ty.OPEN) {
                if (tyLast == MapTsItemVec.Ty.MIDDLE || tyLast == MapTsItemVec.Ty.OPEN) {
                    Merger.tokSeqErr.getAndAdd(1);
                    t.releaseFinite();
                    sub.onError(new RuntimeException("logic"));
                    return;
                }
            }
            else if (ty == MapTsItemVec.Ty.FULL) {
                if (tyLast == MapTsItemVec.Ty.MIDDLE || tyLast == MapTsItemVec.Ty.OPEN) {
                    Merger.tokSeqErr.getAndAdd(1);
                    t.releaseFinite();
                    sub.onError(new RuntimeException("logic"));
                    return;
                }
            }
            else if (ty == MapTsItemVec.Ty.MIDDLE) {
                if (tyLast == MapTsItemVec.Ty.FULL || tyLast == MapTsItemVec.Ty.CLOSE) {
                    Merger.tokSeqErr.getAndAdd(1);
                    t.releaseFinite();
                    sub.onError(new RuntimeException("logic"));
                    return;
                }
            }
            else if (ty == MapTsItemVec.Ty.CLOSE) {
                if (tyLast == MapTsItemVec.Ty.FULL || tyLast == MapTsItemVec.Ty.CLOSE) {
                    Merger.tokSeqErr.getAndAdd(1);
                    t.releaseFinite();
                    sub.onError(new RuntimeException("logic"));
                    return;
                }
            }
        }
        tyLast = ty;
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
