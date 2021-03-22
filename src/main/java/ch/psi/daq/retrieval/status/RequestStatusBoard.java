package ch.psi.daq.retrieval.status;

import ch.psi.daq.retrieval.config.ConfigurationRetrieval;
import ch.psi.daq.retrieval.reqctx.ReqCtx;
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

    public RequestStatusBoard(ConfigurationRetrieval conf) {
        this.conf = conf;
    }

    public Stats stats() {
        return new Stats(this);
    }

    public synchronized RequestStatus get(String reqId) {
        RequestStatus ret = map.get(reqId);
        return ret;
    }

    public synchronized RequestStatus getOrCreate(ReqCtx reqCtx) {
        RequestStatus ret = map.get(reqCtx.reqId);
        if (ret == null) {
            ret = new RequestStatus(reqCtx);
            map.put(reqCtx.reqId, ret);
        }
        else {
            ret.ping();
        }
        return ret;
    }

    public synchronized void requestBegin(ReqCtx reqCtx) {
        getOrCreate(reqCtx);
    }

    public synchronized void requestSubBegin(ReqCtx reqCtx, String mainReqId) {
        getOrCreate(reqCtx);
        reqCtx.mainReqId = mainReqId;
    }

    public synchronized void requestError(ReqCtx reqCtx, Throwable e) {
        getOrCreate(reqCtx).addError(new Error(e));
    }

    public synchronized void requestErrorChannelName(ReqCtx reqCtx, String channelName, Throwable e) {
        getOrCreate(reqCtx).addError(new Error(e, channelName));
    }

    public synchronized void ping(ReqCtx reqCtx) {
        getOrCreate(reqCtx).ping();
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
        LOGGER.debug("RequestStatus bodyEmitted  sig {}  summary {}", sig, getOrCreate(reqCtx).summary());
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

    final Map<String, RequestStatus> map = new TreeMap<>();
    public final ConfigurationRetrieval conf;

}
