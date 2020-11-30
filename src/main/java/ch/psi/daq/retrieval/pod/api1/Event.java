package ch.psi.daq.retrieval.pod.api1;

import com.fasterxml.jackson.databind.JsonNode;

public class Event {
    public Ts ts;
    public long pulse;
    public JsonNode data;
    public Event(long ts, long pulse) {
        this.ts = new Ts(ts);
        this.pulse = pulse;
    }
}
