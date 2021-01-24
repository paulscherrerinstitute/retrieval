package ch.psi.daq.retrieval.merger;

import ch.psi.daq.retrieval.bytes.BufCont;
import ch.qos.logback.classic.Logger;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class SubIn<T extends Comparable<T> & Markable & Releasable> implements Subscriber<T> {
    static final Logger LOGGER = (Logger) LoggerFactory.getLogger(SubIn.class.getSimpleName());
    Subscription sub;
    Queue<T> queue = new ConcurrentLinkedQueue<>();
    AtomicInteger complete = new AtomicInteger();
    AtomicInteger error = new AtomicInteger();
    AtomicInteger subscribed = new AtomicInteger();
    AtomicInteger cancelled = new AtomicInteger();
    AtomicLong requested = new AtomicLong();
    Publisher<T> pub;
    Merger<T> merger;
    final Object mx1 = new Object();
    int sid;
    Throwable except;

    SubIn(int sid, Merger<T> merger, Publisher<T> pub) {
        this.sid = sid;
        this.merger = merger;
        this.pub = pub;
    }

    void subscribe() {
        pub.subscribe(this);
    }

    @Override
    public void onSubscribe(Subscription sub) {
        subscribed.set(1);
        this.sub = sub;
        request(1);
    }

    @Override
    public void onNext(T item) {
        requested.getAndAdd(-1);
        if (cancelled.get() != 0) {
            item.markWith(BufCont.Mark.SUB_IN_CAN_REL);
            item.releaseFinite();
        }
        else {
            item.markWith(BufCont.Mark.SUB_IN);
            queue.add(item);
            if (cancelled.get() != 0) {
                for (T t : queue) {
                    t.releaseFinite();
                }
            }
            merger.produce();
        }
    }

    @Override
    public void onError(Throwable e) {
        LOGGER.error("SubIn got {}", e.toString());
        except = e;
        error.set(1);
        merger.produce();
    }

    @Override
    public void onComplete() {
        complete.set(1);
        merger.produce();
    }

    void request(long n) {
        synchronized (mx1) {
            requested.getAndAdd(n);
            sub.request(n);
        }
    }

    void cancel() {
        synchronized (mx1) {
            if (cancelled.compareAndExchange(0, 1) == 0) {
                sub.cancel();
            }
            for (T t : queue) {
                t.releaseFinite();
            }
        }
    }

}
