package ch.psi.daq.retrieval.pod.api1;

import java.util.Map;

public class EventAggregated {
    public Ts ts;
    public long pulse;
    public long eventCount;
    public Map<String, AggResult> data;
    public EventAggregated(long ts, long pulse, long eventCount) {
        this.ts = new Ts(ts);
        this.pulse = pulse;
        this.eventCount = eventCount;
    }
}
