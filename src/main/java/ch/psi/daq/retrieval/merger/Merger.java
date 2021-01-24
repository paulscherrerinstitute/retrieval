package ch.psi.daq.retrieval.merger;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class Merger<T extends Comparable<T> & Markable & Releasable> implements Publisher<T> {
    public static class AlreadySubscribed extends RuntimeException {}
    public static class UpstreamError extends RuntimeException {
        UpstreamError() {}
        UpstreamError(Throwable e) { super(e); }
    }
    static final AtomicLong subscribeCount = new AtomicLong();

    public static class Stats {
        public long subscribeCount;
        public Stats() {
            subscribeCount = Merger.subscribeCount.get();
        }
    }

    public static <T extends Comparable<T> & Markable & Releasable> Merger<T> merge(List<Publisher<T>> pubs) {
        Merger<T> ret = new Merger<>();
        AtomicInteger i = new AtomicInteger();
        ret.subInps = pubs.stream().map(k -> new SubIn<T>(i.getAndAdd(1), ret, k)).collect(Collectors.toList());
        return ret;
    }

    List<SubIn<T>> subInps;
    SubOut<T> subOut;
    AtomicInteger subscribed = new AtomicInteger();
    AtomicInteger producing = new AtomicInteger();
    AtomicInteger term = new AtomicInteger();

    Merger() {}

    public void subscribe(Subscriber<? super T> sub) {
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
                subOut = new SubOut<T>(this, sub);
                sub.onSubscribe(subOut);
                subInps.forEach(SubIn::subscribe);
            }
        }
    }

    void produce() {
        if (producing.getAndAdd(1) <= 0) {
            while (true) {
                while (true) {
                    if (term.get() != 0) {
                        break;
                    }
                    if (subOut.cancelled.get() != 0) {
                        break;
                    }
                    if (subOut.requested.get() <= 0) {
                        break;
                    }
                    if (!produce2()) {
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
        Throwable except = null;
        for (SubIn<T> inp : subInps) {
            if (inp.subscribed.get() != 1) {
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
            return false;
        }
        else if (allInfo) {
            T g = null;
            int ix = -1;
            int i1 = 0;
            for (SubIn<T> inp : subInps) {
                if (inp.queue.size() > 0) {
                    if (g == null || inp.queue.peek().compareTo(g) <= 0) {
                        g = inp.queue.peek();
                        ix = i1;
                    }
                }
                i1 += 1;
            }
            if (ix >= 0) {
                T t = subInps.get(ix).queue.remove();
                if (subOut.requested.getAndAdd(-1) > 0) {
                    subOut.signalNext(t);
                }
                else {
                    term.set(1);
                    subOut.signalError(new RuntimeException("logic"));
                    return false;
                }
                return true;
            }
            else {
                if (term.compareAndExchange(0, 1) == 0) {
                    subOut.signalComplete();
                }
                return false;
            }
        }
        else {
            return false;
        }
    }

    void downstreamCancelled() {
        for (SubIn<T> inp : subInps) {
            inp.cancel();
        }
    }

}
