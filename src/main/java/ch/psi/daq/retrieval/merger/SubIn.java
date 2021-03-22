package ch.psi.daq.retrieval.merger;

import ch.psi.daq.retrieval.bytes.BufCont;
import ch.psi.daq.retrieval.eventmap.ts.MapTsToken;
import ch.qos.logback.classic.Logger;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class SubIn<T extends Comparable<T> & Markable & Releasable & MergeToken> implements Subscriber<T> {
    static final Logger LOGGER = (Logger) LoggerFactory.getLogger(SubIn.class.getSimpleName());

    SubIn(int sid, Merger<T> merger, Publisher<T> pub) {
        synchronized (merger) {
            this.sid = sid;
            this.merger = merger;
            this.pub = pub;
        }
    }

    void subscribe() {
        synchronized (merger) {
            pub.subscribe(this);
        }
    }

    @Override
    public void onSubscribe(Subscription scr) {
        synchronized (merger) {
            if (cancelled.get() == 0) {
                subscribed.set(1);
                this.subRef.set(scr);
                request(1);
            }
            else {
                scr.cancel();
            }
        }
    }

    @Override
    public void onNext(T item) {
        synchronized (merger) {
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
    }

    @Override
    public void onError(Throwable e) {
        synchronized (merger) {
            LOGGER.error("SubIn got {}", e.toString());
            except = e;
            error.set(1);
            merger.produce();
        }
    }

    @Override
    public void onComplete() {
        synchronized (merger) {
            complete.set(1);
            merger.produce();
        }
    }

    void request(long n) {
        synchronized (merger) {
            requested.getAndAdd(n);
            Subscription scr = subRef.get();
            if (scr == null) {
            }
            else {
                scr.request(n);
            }
        }
    }

    void cancel() {
        if (cancelled.compareAndExchange(0, 1) == 0) {
            Subscription scr = subRef.get();
            if (scr == null) {
            }
            else {
                scr.cancel();
            }
        }
        while (true) {
            T t = queue.poll();
            if (t == null) {
                break;
            }
            else {
                t.releaseFinite();
            }
        }
    }

    AtomicReference<Subscription> subRef = new AtomicReference<>();
    final Queue<T> queue = new ConcurrentLinkedQueue<>();
    AtomicInteger complete = new AtomicInteger();
    AtomicInteger error = new AtomicInteger();
    AtomicInteger subscribed = new AtomicInteger();
    AtomicInteger cancelled = new AtomicInteger();
    AtomicLong requested = new AtomicLong();
    final Publisher<T> pub;
    final Merger<T> merger;
    final int sid;
    Throwable except;

}
