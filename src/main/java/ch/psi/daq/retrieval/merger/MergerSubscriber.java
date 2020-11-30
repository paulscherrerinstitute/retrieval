package ch.psi.daq.retrieval.merger;

import ch.psi.daq.retrieval.eventmap.ts.Item;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class MergerSubscriber implements Subscriber<Item> {
    Logger LOGGER = (Logger) LoggerFactory.getLogger("MergerSubscriber");
    final Merger merger;
    final String channelName;
    Subscription sub;
    Item item;
    int id;
    boolean subError;
    boolean subComplete;
    long seenBytesFromUpstream;
    boolean itemTerm;
    State state = State.Fresh;
    AtomicLong nreq = new AtomicLong();
    AtomicLong nConstructed = new AtomicLong();
    AtomicLong nSubscribed = new AtomicLong();
    AtomicLong nReleased = new AtomicLong();
    static Marker markerTrack = MarkerFactory.getMarker("MergerSubscriberTrack");

    public enum State {
        Fresh,
        Subscribed,
        Terminated,
        Released,
    }

    MergerSubscriber(Merger merger, String channelName, int id) {
        Level level = LOGGER.getEffectiveLevel();
        LOGGER = (Logger) LoggerFactory.getLogger(String.format("MergerSubscriber_%02d", id));
        LOGGER.setLevel(level);
        this.merger = merger;
        this.channelName = channelName;
        this.id = id;
        this.nreq.set(0);
        nConstructed.getAndAdd(1);
    }

    @Override
    public void onSubscribe(Subscription sub) {
        nSubscribed.getAndAdd(1);
        synchronized (merger) {
            LOGGER.debug("{}  onSubscribe  {}", id, channelName);
            if (sub == null) {
                LOGGER.error(String.format("Publisher for id %d passed null Subscription", id));
                merger.selfError(new RuntimeException(String.format("Publisher for id %d passed null Subscription", id)));
                return;
            }
            if (state != State.Fresh) {
                LOGGER.error("onSubscribe but state != State.Fresh");
                merger.selfError(new RuntimeException("onSubscribe but state != State.Fresh"));
                return;
            }
            this.sub = sub;
            state = State.Subscribed;
        }
    }

    @Override
    public void onNext(Item it) {
        synchronized (merger) {
            LOGGER.trace(markerTrack, "{}  onNext  nreq {}  item {}", id, nreq, it);
            try {
                if (state != State.Subscribed) {
                    LOGGER.warn(String.format("onNext  id %d  but not Subscribed, ignoring.", id));
                    return;
                }
                if (nreq.get() <= 0) {
                    LOGGER.info("{}  next unexpected nreq {}", id, nreq);
                    merger.selfError(new RuntimeException("logic"));
                    return;
                }
                nreq.addAndGet(-1);
                long bs = 0;
                if (it.item1 != null) {
                    if (it.item1.buf != null) {
                        bs += it.item1.buf.readableByteCount();
                    }
                }
                if (it.item2 != null) {
                    if (it.item2.buf != null) {
                        bs += it.item2.buf.readableByteCount();
                    }
                }
                if (it.item1 != null && it.item1.buf != null) {
                    long n = it.item1.buf.readableByteCount();
                    seenBytesFromUpstream += n;
                    merger.totalSeenBytesFromUpstream += n;
                }
                if (it.item2 != null && it.item2.buf != null) {
                    long n = it.item2.buf.readableByteCount();
                    seenBytesFromUpstream += n;
                    merger.totalSeenBytesFromUpstream += n;
                }
                if (item != null) {
                    item.release();
                    item = null;
                }
                if (it.isTerm()) {
                    itemTerm = true;
                }
                else if (!it.isPlainBuffer() && !it.hasMoreMarkers()) {
                    LOGGER.trace("NOT PLAIN AND NO MORE MARKERS");
                }
                else {
                    if (!it.verify()) {
                        merger.selfError(new RuntimeException("bad item"));
                        return;
                    }
                    item = it;
                    it = null;
                }
                merger.next(id);
            }
            catch (Throwable e) {
                LOGGER.error("error in call to merger.next  id {}  {}", id, e.toString());
            }
            finally {
                if (it != null) {
                    it.release();
                }
            }
        }
    }

    @Override
    public void onError(Throwable e) {
        synchronized (merger) {
            if (state != State.Subscribed && state != State.Terminated) {
                LOGGER.error("{}  onError received in invalid state  {}", id, channelName);
                return;
            }
            if (state != State.Subscribed) {
                LOGGER.error("{}  onError received while not subscribed  {}", id, channelName);
                return;
            }
            StringBuilder sb = new StringBuilder();
            merger.formatState(sb);
            LOGGER.error("{}  onError in {}: {}\nmerger.formatState:\n{}", id, channelName, e.toString(), sb.toString());
            subError = true;
            state = State.Terminated;
            nreq.set(0);
            merger.selfError(e);
        }
    }

    @Override
    public void onComplete() {
        synchronized (merger) {
            if (state != State.Subscribed && state != State.Terminated) {
                LOGGER.error("onComplete received in invalid state  {}", channelName);
                return;
            }
            if (state != State.Subscribed) {
                LOGGER.error("onComplete received even though not subscribed  {}", channelName);
                return;
            }
            LOGGER.debug("{}  onComplete  nreq {}  seenBytesFromUpstream {}  merger.totalSeenBytesFromUpstream {}", id, nreq, seenBytesFromUpstream, merger.totalSeenBytesFromUpstream);
            subComplete = true;
            state = State.Terminated;
            nreq.set(0);
            merger.signal(id);
        }
    }

    public boolean maybeMoreItems() {
        synchronized (merger) {
            return state == State.Subscribed && !itemTerm;
        }
    }

    public void request() {
        LOGGER.trace(markerTrack, "request() {}  {}  BEFORE SYNC", id, channelName);
        synchronized (merger) {
            LOGGER.trace(markerTrack, "request() {}  {}  INSIDE SYNC", id, channelName);
            if (state != State.Subscribed) {
                LOGGER.info("request called without Subscribed");
                return;
            }
            LOGGER.trace(markerTrack, "{}  request FORWARD", id);
            nreq.addAndGet(1);
            sub.request(1);
            LOGGER.trace(markerTrack, "{}  request END", id);
        }
    }

    public long nreq() {
        synchronized (merger) {
            return nreq.get();
        }
    }

    public void cancel() {
        synchronized (merger) {
            LOGGER.warn("cancel() {}  {}", id, channelName);
            state = State.Terminated;
            if (sub != null) {
                sub.cancel();
                sub = null;
            }
            else {
                LOGGER.warn("cancel even though never active");
            }
        }
    }

    public void formatState(StringBuilder sb) {
        String s1 = "null";
        if (item != null) {
            if (item.isPlainBuffer()) {
                s1 = "isPlainBuffer";
            }
            else if (item.hasMoreMarkers()) {
                s1 = "hasMoreMarkers";
            }
            else {
                s1 = "otherNonNull";
            }
        }
        sb.append(String.format("MS %2d  state %s  has item %s  subComplete %s  subError %s   %s", id, state, s1, subComplete, subError, item));
    }

    public boolean hasItem() {
        synchronized (merger) {
            return item != null;
        }
    }

    public boolean hasMoreMarkers() {
        synchronized (merger) {
            return item != null && item.hasMoreMarkers();
        }
    }

    public Item getItem() {
        synchronized (merger) {
            return item;
        }
    }

    public void itemAdvOrRemove() {
        synchronized (merger) {
            if (item != null) {
                item.adv();
                if (item.item1 == null) {
                    item = null;
                }
            }
        }
    }

    public void releaseItem() {
        synchronized (merger) {
            if (item != null) {
                item.release();
                item = null;
            }
        }
    }

    public void release() {
        nReleased.getAndAdd(1);
        if (state == State.Released) {
            LOGGER.warn("already Released");
        }
        if (sub != null) {
            sub.cancel();
            sub = null;
        }
        if (item != null) {
            item.release();
            item = null;
        }
        state = State.Released;
    }

}
