package ch.psi.daq.retrieval.merger;

import ch.qos.logback.classic.Logger;
import org.reactivestreams.Subscription;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class MergerSubscription implements Subscription {
    static Logger LOGGER = (Logger) LoggerFactory.getLogger("MergerSubscription");
    static Marker markerTrack = MarkerFactory.getMarker("MergerSubscriptionTrack");
    Merger merger;
    AtomicInteger requestDepth = new AtomicInteger();

    MergerSubscription(Merger merger) {
        this.merger = merger;
    }

    @Override
    public void request(long n) {
        int inReq = requestDepth.getAndAdd(1);
        try {
            LOGGER.trace(markerTrack, "MergerSubscription::request  BEGIN  inReq {}  n {}", inReq, n);
            merger.request(n, inReq);
            LOGGER.trace(markerTrack, "MergerSubscription::request  END    inReq {}  n {}", inReq, n);
        }
        catch (Throwable e) {
            LOGGER.error("error in request()  {}", e.toString());
        }
        finally {
            requestDepth.getAndAdd(-1);
        }
    }

    @Override
    public void cancel() {
        LOGGER.warn("cancel  {}", merger.channelName);
        merger.cancel();
    }

}
