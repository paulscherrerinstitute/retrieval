package ch.psi.daq.retrieval.merger;

import ch.psi.daq.retrieval.eventmap.ts.MapTsItemVec;
import ch.psi.daq.retrieval.utils.Tools;
import ch.qos.logback.classic.Logger;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class Merger<T extends Comparable<T> & Markable & Releasable & MergeToken> implements Publisher<T> {
    static final Logger LOGGER = (Logger) LoggerFactory.getLogger(Merger.class.getSimpleName());
    public static class AlreadySubscribed extends RuntimeException {}
    public static class UpstreamError extends RuntimeException {
        UpstreamError() {}
        UpstreamError(Throwable e) { super(e); }
    }

    public static <T extends Comparable<T> & Markable & Releasable & MergeToken> Merger<T> merge(List<Publisher<T>> pubs, long endNanos, String name) {
        Merger<T> ret = new Merger<>(endNanos, name);
        AtomicInteger i = new AtomicInteger();
        pubs.forEach(k -> {
            ret.subInps.add(new SubIn<>(i.getAndAdd(1), ret, k));
        });
        return ret;
    }

    public synchronized void subscribe(Subscriber<? super T> sub) {
        if (sub == null) {
            throw new NullPointerException();
        }
        if (subscribed.compareAndExchange(0, 1) != 0) {
            sub.onError(new Merger.AlreadySubscribed());
        }
        else {
            subscribeCount.getAndAdd(1);
            if (subOut != null) {
                sub.onError(new Merger.AlreadySubscribed());
            }
            else {
                subOut = new SubOut<>(this, sub);
                sub.onSubscribe(subOut);
                subInps.forEach(SubIn::subscribe);
                wd = new Thread(this::watchdog);
                wd.start();
            }
        }
    }

    void produce() {
        if (producing.getAndAdd(1) <= 0) {
            while (true) {
                while (true) {
                    actionLast.set(System.nanoTime());
                    if (term.get() != 0) {
                        produceLastBreakReason.set(1);
                        break;
                    }
                    if (subOut.cancelled.get() != 0) {
                        produceLastBreakReason.set(2);
                        break;
                    }
                    if (subOut.requested.get() <= 0) {
                        produceLastBreakReason.set(3);
                        break;
                    }
                    if (!produce2()) {
                        produceLastBreakReason.set(4);
                        break;
                    }
                }
                if (producing.getAndAdd(-1) <= 1) {
                    break;
                }
            }
        }
    }

    boolean produce2() {
        boolean allInfo = true;
        boolean anyEmptyWithError = false;
        boolean anyEmptyComplete = false;
        Throwable except = null;
        for (SubIn<T> inp : subInps) {
            if (inp.subscribed.get() == 0) {
                allInfo = false;
            }
            else if (inp.queue.size() < 1) {
                if (inp.error.get() != 0) {
                    allInfo = false;
                    anyEmptyWithError = true;
                    except = inp.except;
                }
                else if (inp.complete.get() == 0) {
                    allInfo = false;
                    if (inp.requested.get() < 1) {
                        inp.request(8);
                    }
                }
                else if (inp.complete.get() != 0) {
                    anyEmptyComplete = true;
                }
            }
        }
        if (anyEmptyWithError) {
            for (SubIn<T> inp : subInps) {
                inp.cancel();
            }
            if (term.compareAndExchange(0, 1) == 0) {
                if (except != null) {
                    subOut.signalError(new UpstreamError(except));
                }
                else {
                    subOut.signalError(new UpstreamError());
                }
            }
            produce2Return.set(1);
            return false;
        }
        else if (stopOnAnyCompleteEmpty && anyEmptyComplete) {
            emptyCompleteInput.getAndIncrement();
            for (SubIn<T> inp : subInps) {
                inp.cancel();
            }
            if (term.compareAndExchange(0, 1) == 0) {
                //subOut.signalError(new RuntimeException("emptyCompleteInput"));
                subOut.signalComplete();
            }
            produce2Return.set(2);
            return false;
        }
        else if (allInfo) {
            int ix = -1;
            {
                T g = null;
                int i1 = 0;
                for (SubIn<T> inp : subInps) {
                    if (inp.queue.size() > 0) {
                        T j = inp.queue.peek();
                        if (g == null || j.compareTo(g) <= 0) {
                            g = j;
                            ix = i1;
                        }
                    }
                    i1 += 1;
                }
            }
            if (pickMode == 1) {
                if (subInps.get(pickIndex).queue.size() <= 0) {
                    subOut.signalError(new UpstreamError());
                }
                else if (ix != pickIndex) {
                    pickDiffer.getAndAdd(1);
                    ix = pickIndex;
                }
            }
            if (ix >= 0) {
                T t = subInps.get(ix).queue.peek();
                if (t.ts() >= endNanos) {
                    //LOGGER.info("{}  chosen ix {}  beyond endNanos {}", name, ix, endNanos);
                    for (SubIn<T> inp : subInps) {
                        inp.cancel();
                    }
                    if (term.compareAndExchange(0, 1) == 0) {
                        completeAllBeyondEnd.getAndIncrement();
                        subOut.signalComplete();
                    }
                    else {
                        attemptTermAgain.getAndIncrement();
                    }
                    produce2Return.set(3);
                    return false;
                }
                else {
                    t = subInps.get(ix).queue.remove();
                    if (subOut.requested.getAndAdd(-1) > 0) {
                        if (t.ty() == MapTsItemVec.Ty.OPEN) {
                            pickMode = 1;
                            pickIndex = ix;
                            pickEnter.getAndAdd(1);
                        }
                        else if (t.ty() == MapTsItemVec.Ty.CLOSE) {
                            pickMode = 0;
                            pickIndex = -1;
                            pickExit.getAndAdd(1);
                        }
                        subOut.signalNext(t);
                        produce2Return.set(4);
                        return true;
                    }
                    else {
                        term.set(1);
                        subOut.signalError(new RuntimeException("logic"));
                        produce2Return.set(5);
                        return false;
                    }
                }
            }
            else {
                for (SubIn<T> inp : subInps) {
                    inp.cancel();
                }
                if (term.compareAndExchange(0, 1) == 0) {
                    //LOGGER.info("{}  no ix found COMPLETE?", name);
                    subOut.signalComplete();
                }
                produce2Return.set(6);
                return false;
            }
        }
        else {
            produce2Return.set(7);
            return false;
        }
    }

    void downstreamCancel() {
        cancelled.set(1);
        for (SubIn<T> inp : subInps) {
            inp.cancel();
        }
    }

    void watchdog() {
        int outCount = 0;
        wdCount.getAndAdd(1);
        long lastPrint = System.nanoTime();
        while (true) {
            Tools.sleep(4000);
            if (term.get() != 0) {
                break;
            }
            if (cancelled.get() != 0) {
                break;
            }
            long nowMs = System.nanoTime() / 1000000L;
            long actMs = actionLast.get() / 1000000L;
            long dt = nowMs - actMs;
            if (dt > 10000) {
                outCount += 1;
                LOGGER.warn("[{}]  WD  action timeout  producing {}  produceLastBreakReason {}  produce2Return {}  subreq {}  dt {}  cancelled {}  error {}", name, producing.get(), produceLastBreakReason.get(), produce2Return.get(), subOut.requested.get(), dt, subOut.cancelled.get(), subOut.error.get());
                synchronized (subInps) {
                    int i1 = 0;
                    for (SubIn<T> subInp : subInps) {
                        LOGGER.warn("[{}]  WD  subInp {}  queue {}  complete {}  cancelled {}  requested {}", name, i1, subInp.queue.size(), subInp.complete.get(), subInp.cancelled.get(), subInp.requested.get());
                        i1 += 1;
                    }
                }
                lastPrint = System.nanoTime();
            }
            if (outCount > 6) {
                LOGGER.warn("[{}]  WD  quit  outCount {}", name, outCount);
                break;
            }
        }
        wdCount.getAndAdd(-1);
    }

    Merger(long endNanos, String name) {
        this.endNanos = endNanos;
        this.name = name;
    }

    public static class Stats {
        public long subscribeCount;
        public long wdCount;
        public long pickEnter;
        public long pickExit;
        public long pickDiffer;
        public long tokSeqErr;
        public long emptyCompleteInput;
        public long completeAllBeyondEnd;
        public long attemptTermAgain;
        public Stats() {
            subscribeCount = Merger.subscribeCount.get();
            wdCount = Merger.wdCount.get();
            pickEnter = Merger.pickEnter.get();
            pickExit = Merger.pickExit.get();
            pickDiffer = Merger.pickDiffer.get();
            tokSeqErr = Merger.tokSeqErr.get();
            emptyCompleteInput = Merger.emptyCompleteInput.get();
            completeAllBeyondEnd = Merger.completeAllBeyondEnd.get();
            attemptTermAgain = Merger.attemptTermAgain.get();
        }
    }

    static final AtomicLong subscribeCount = new AtomicLong();
    static final AtomicLong wdCount = new AtomicLong();
    static final AtomicLong pickEnter = new AtomicLong();
    static final AtomicLong pickExit = new AtomicLong();
    static final AtomicLong pickDiffer = new AtomicLong();
    static final AtomicLong tokSeqErr = new AtomicLong();
    static final AtomicLong emptyCompleteInput = new AtomicLong();
    static final AtomicLong completeAllBeyondEnd = new AtomicLong();
    static final AtomicLong attemptTermAgain = new AtomicLong();
    final String name;
    final List<SubIn<T>> subInps = new ArrayList<>();
    SubOut<T> subOut;
    final AtomicInteger subscribed = new AtomicInteger();
    final AtomicInteger producing = new AtomicInteger();
    final AtomicInteger term = new AtomicInteger();
    final AtomicInteger cancelled = new AtomicInteger();
    final AtomicLong actionLast = new AtomicLong();
    final AtomicInteger produceLastBreakReason = new AtomicInteger();
    final AtomicInteger produce2Return = new AtomicInteger();
    final long endNanos;
    Thread wd;
    int pickMode;
    int pickIndex = -1;
    boolean stopOnAnyCompleteEmpty;

}
