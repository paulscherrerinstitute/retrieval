package ch.psi.daq.retrieval.controller;

import ch.psi.daq.retrieval.bytes.BufCont;
import ch.psi.daq.retrieval.merger.Releasable;
import ch.qos.logback.classic.Logger;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class Parts implements Subscriber<BufCont>, Subscription, Publisher<BufCont>, Releasable {
    static final Logger LOGGER = (Logger) LoggerFactory.getLogger(Parts.class.getSimpleName());

    public static Parts fromPublisher(Publisher<BufCont> k, String name) {
        Parts ret = new Parts(name);
        ret.pub = k;
        return ret;
    }

    Parts(String name) {
        this.name = Objects.requireNonNull(name);
    }

    @Override
    public synchronized void subscribe(Subscriber<? super BufCont> k) {
        int h = subscribeCount.getAndAdd(1);
        LOGGER.debug(markPartsTrace, "[{}]  subscribe  subscribeCount {}", name, h);
        if (complete.get() != 0) {
            k.onComplete();
        }
        else if (error.get() != 0) {
            k.onError(new RuntimeException("UpstreamException"));
        }
        else if (subRef.get() != null) {
            LOGGER.error("[{}]  subscribe called even though still subscribed", name);
            k.onError(new RuntimeException("subscribe called even though still subscribed"));
        }
        else if (h == 0) {
            LOGGER.debug(markPartsTrace, "[{}]  subscribing to upstream", name);
            subRef.set(k);
            pub.subscribe(this);
        }
        else if (h == 1) {
            LOGGER.debug(markPartsTrace, "[{}]  not subscribing 2nd time to upstream", name);
            subRef.set(k);
            requested.set(0);
            k.onSubscribe(this);
        }
        else {
            subscribeCount.set(100);
            LOGGER.error("[{}]  later subscribe not possible", name);
        }
    }

    @Override
    public synchronized void onSubscribe(Subscription k) {
        int h = onSubscribeCount.getAndAdd(1);
        if (released.get() != 0) {
            LOGGER.warn("[{}]  onSubscribe  onSubscribeCount {}  but already released", name, h);
            k.cancel();
        }
        else if (cancelledSelf.get() != 0) {
            LOGGER.warn("[{}]  onSubscribe  onSubscribeCount {}  but Parts already cancelled", name, h);
            k.cancel();
        }
        else {
            LOGGER.debug(markPartsTrace, "[{}]  onSubscribe  onSubscribeCount {}", name, h);
            scr = k;
            Subscriber<? super BufCont> sub = subRef.get();
            if (sub != null) {
                sub.onSubscribe(this);
            }
            // TODO any case where I need to request from upstream at this point?
        }
    }

    @Override
    public synchronized void onNext(BufCont k) {
        k.appendMark(BufCont.Mark.Parts1);
        Subscriber<? super BufCont> sub = subRef.get();
        long rup = reqUp.getAndAdd(-1);
        if (rup < 1) {
            LOGGER.error("[{}]  onNext even though not requested {}", name, rup);
            if (sub != null) {
                sub.onError(new RuntimeException("about to onNext but no demand"));
            }
            else {
                LOGGER.error("[{}]  onNext can not signal error, no sub", name);
            }
        }
        else {
            if (sub != null) {
                int w = producing.getAndAdd(1);
                if (w != 0) {
                    LOGGER.error("[{}]  onNext  BUT ALREADY PRODUCING", name);
                }
                k.appendMark(BufCont.Mark.Parts2);
                int rb = k.readableByteCount();
                long j = bytesEmitted.addAndGet(rb);
                sub.onNext(k);
                int z = producing.getAndAdd(-1);
                if (z != 1) {
                    LOGGER.error("[{}]  onNext  overlapping produce", name);
                }
                if (blockMore()) {
                    if (rup == 1) {
                        LOGGER.trace(markPartsTrace, "[{}]  byte limit reached  {}", name, j);
                        subRef.set(null);
                        sub.onComplete();
                    }
                    else {
                        LOGGER.trace(markPartsTrace, "[{}]  byte limit reached  {}   BUT IN FLIGHT", name, j);
                    }
                }
                requestMaybe();
            }
            else {
                k.appendMark(BufCont.Mark.PartsNextDIS);
                LOGGER.error("[{}]  onNext but disconnected sub", name);
            }
        }
    }

    synchronized boolean blockMore() {
        return subscribeCount.get() == 1 && bytesEmitted.get() > 200;
    }

    synchronized void requestMaybe() {
        if (cancelledUp.get() == 0 && !blockMore()) {
            long u = Math.max(0, Math.min(12, requested.get()));
            if (u > 0) {
                long y = requested.getAndAdd(-u);
                if (y < u) {
                    requested.getAndAdd(u - y);
                    //LOGGER.info("[{}]  requesting {}  y {}", name, u, y);
                    reqUp.getAndAdd(y);
                    scr.request(y);
                }
                else {
                    //LOGGER.info("[{}]  requesting {}", name, u);
                    reqUp.getAndAdd(u);
                    scr.request(u);
                }
            }
        }
    }

    @Override
    public synchronized void onError(Throwable e) {
        LOGGER.error("[{}]  onError {}", name, e.toString());
        error.getAndSet(1);
        Subscriber<? super BufCont> sub = subRef.get();
        if (sub != null) {
            LOGGER.error("[{}]  onError PASS ON {}", name, e.toString());
            subRef.set(null);
            sub.onError(e);
        }
    }

    @Override
    public synchronized void onComplete() {
        LOGGER.trace(markPartsTrace, "[{}]  onComplete", name);
        complete.getAndSet(1);
        Subscriber<? super BufCont> sub = subRef.get();
        if (sub != null) {
            subRef.set(null);
            LOGGER.trace(markPartsTrace, "[{}]  onComplete  signal to sub", name);
            sub.onComplete();
        }
    }

    @Override
    public synchronized void request(long n) {
        //LOGGER.info("[{}]  request {}", name, n);
        Subscriber<? super BufCont> sub = subRef.get();
        if (sub == null) {
            LOGGER.error("[{}]  got request without subscriber  {}  {}", name, n, state());
        }
        // TODO reconsider the branches
        if (subscribeCount.get() == 1 && n != 1) {
            LOGGER.error("[{}]  bad request for phase {}", name, n);
            cancelUp();
            if (sub != null) {
                sub.onError(new RuntimeException("bad request"));
                subRef.set(null);
            }
        }
        else if (n > 100) {
            LOGGER.error("[{}]  too much requested {}", name, n);
            cancelUp();
            if (sub != null) {
                sub.onError(new RuntimeException("too much requested"));
                subRef.set(null);
            }
        }
        else {
            if (n < Long.MAX_VALUE - requested.get()) {
                requested.getAndAdd(n);
            }
            else {
                LOGGER.error("[{}]  request {}  now unbounded", name, n);
                if (sub != null) {
                    sub.onError(new RuntimeException(String.format("[%s]  unbounded request not supported", name)));
                    subRef.set(null);
                }
            }
            if (producing.get() == 0) {
                requestMaybe();
            }
        }
    }

    @Override
    public void cancel() {
        if (cancelledSelf.compareAndExchange(0, 1) == 0) {
        }
        cancelUp();
    }

    void cancelUp() {
        if (cancelledUp.compareAndExchange(0, 1) == 0) {
            if (scr != null) {
                LOGGER.debug("[{}]  cancelUp  pass on", name);
                scr.cancel();
            }
            else {
                LOGGER.warn("[{}]  cancelUp  scr is null", name);
            }
        }
        else {
            LOGGER.warn("[{}]  cancelUp  already cancelled", name);
        }
    }

    public void release() {
        if (released.compareAndExchange(0, 1) == 0) {
            cancel();
        }
    }

    String state() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("  requested %d", requested.get()));
        sb.append(String.format("  reqUp %d", reqUp.get()));
        sb.append(String.format("  bytesEmitted %d", bytesEmitted.get()));
        sb.append(String.format("  cancelledSelf %d", cancelledSelf.get()));
        sb.append(String.format("  cancelledUp %d", cancelledUp.get()));
        sb.append(String.format("  producing %d", producing.get()));
        sb.append(String.format("  subscribeCount %d", subscribeCount.get()));
        sb.append(String.format("  onSubscribeCount %d", onSubscribeCount.get()));
        sb.append(String.format("  complete %d", complete.get()));
        sb.append(String.format("  error %d", error.get()));
        sb.append(String.format("  released %d", released.get()));
        return sb.toString();
    }

    @Override
    public void releaseFinite() {
        release();
    }

    final String name;
    AtomicInteger cancelledUp = new AtomicInteger();
    AtomicInteger cancelledSelf = new AtomicInteger();
    AtomicInteger producing = new AtomicInteger();
    AtomicInteger subscribeCount = new AtomicInteger();
    AtomicInteger onSubscribeCount = new AtomicInteger();
    AtomicInteger complete = new AtomicInteger();
    AtomicInteger error = new AtomicInteger();
    AtomicInteger released = new AtomicInteger();
    AtomicLong requested = new AtomicLong();
    AtomicLong reqUp = new AtomicLong();
    AtomicLong bytesEmitted = new AtomicLong();
    AtomicReference<Subscriber<? super BufCont>> subRef = new AtomicReference<>();
    Publisher<BufCont> pub;
    Subscription scr;
    Marker markPartsTrace = ItemFilter.markPartsTrace;

}
