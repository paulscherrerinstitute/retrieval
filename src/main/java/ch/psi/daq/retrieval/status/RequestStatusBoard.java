package ch.psi.daq.retrieval.status;

import ch.psi.daq.retrieval.ReqCtx;
import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.SignalType;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

public class RequestStatusBoard {
    static final Logger LOGGER = (Logger) LoggerFactory.getLogger(RequestStatusBoard.class.getSimpleName());
    static final AtomicLong finallyCompleteCount = new AtomicLong();
    static final AtomicLong finallyErrorCount = new AtomicLong();
    static final AtomicLong finallyCancelCount = new AtomicLong();

    public static class Stats {
        public int entryCount;
        public long finallyCompleteCount;
        public long finallyErrorCount;
        public long finallyCancelCount;
        public Stats(RequestStatusBoard rsb) {
            synchronized (rsb.map) {
                entryCount = rsb.map.size();
            }
            finallyCompleteCount = RequestStatusBoard.finallyCompleteCount.get();
            finallyErrorCount = RequestStatusBoard.finallyErrorCount.get();
            finallyCancelCount = RequestStatusBoard.finallyCancelCount.get();
        }
    }

    public Stats stats() {
        return new Stats(this);
    }

    Map<String, RequestStatus> map = new TreeMap<>();

    public synchronized RequestStatus get(String reqId) {
        return map.get(reqId);
    }

    public synchronized void requestBegin(ReqCtx reqctx) {
        map.put(reqctx.reqId, new RequestStatus(reqctx));
    }

    public synchronized void requestSubBegin(ReqCtx reqctx, String mainReqId) {
        reqctx.mainReqId = mainReqId;
        map.put(reqctx.reqId, new RequestStatus(reqctx));
    }

    public synchronized RequestStatus getOrCreate(ReqCtx reqctx) {
        RequestStatus status = map.get(reqctx.reqId);
        if (status == null) {
            status = new RequestStatus(reqctx);
            map.put(reqctx.reqId, status);
        }
        else {
            status.ping();
        }
        return status;
    }

    public synchronized void requestError(ReqCtx reqctx, Throwable e) {
        getOrCreate(reqctx).addError(new RequestStatus.Error(String.format("%s", e.toString())));
    }

    public synchronized void requestErrorChannelName(ReqCtx reqctx, String channelName, Throwable e) {
        getOrCreate(reqctx).addError(new RequestStatus.Error(String.format("channel %s   %s", channelName, e.toString())));
    }

    public synchronized void ping(ReqCtx reqctx) {
        getOrCreate(reqctx).ping();
    }

    public void bodyEmitted(ReqCtx reqCtx, SignalType sig) {
        if (sig == SignalType.ON_COMPLETE) {
            finallyCompleteCount.getAndAdd(1);
        }
        else if (sig == SignalType.ON_ERROR) {
            finallyErrorCount.getAndAdd(1);
        }
        else if (sig == SignalType.CANCEL) {
            finallyCancelCount.getAndAdd(1);
        }
        reqCtx.bodyEmitted();
        ping(reqCtx);
        LOGGER.info("RequestStatus bodyEmitted  sig {}  summary {}", sig, getOrCreate(reqCtx).summary());
    }

    public synchronized int mapCount() {
        return map.size();
    }

    public synchronized long gc() {
        return clean(ZonedDateTime.now(ZoneOffset.UTC).minusSeconds(120), 1000);
    }

    public synchronized long clean(ZonedDateTime keepTo, int keepMax) {
        List<ZonedDateTime> tss = new ArrayList<>();
        for (String k : map.keySet()) {
            RequestStatus s = map.get(k);
            tss.add(s.tsl);
        }
        tss.sort((a, b) -> {
            if (a.equals(b)) {
                return 0;
            }
            if (a.isAfter(b)) {
                return -1;
            }
            return +1;
        });
        int i1 = 0;
        ZonedDateTime thresh = null;
        for (ZonedDateTime t1 : tss) {
            if (i1 >= keepMax || t1.isBefore(keepTo)) {
                thresh = t1;
                break;
            }
            i1 += 1;
        }
        if (thresh != null) {
            List<String> l1 = new ArrayList<>();
            for (String k : map.keySet()) {
                RequestStatus s = map.get(k);
                if (s.tsl.isBefore(thresh) || s.tsl.isEqual(thresh)) {
                    l1.add(k);
                }
            }
            for (String k : l1) {
                map.remove(k);
            }
            return l1.size();
        }
        else {
            return 0;
        }
    }

}
