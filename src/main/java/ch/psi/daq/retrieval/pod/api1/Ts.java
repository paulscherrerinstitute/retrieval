package ch.psi.daq.retrieval.pod.api1;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Ts {

    public Ts() {}

    /*
    @JsonCreator
    public Ts(@JsonProperty(required = true) long sec, @JsonProperty(required = true) long ns) {
        this.sec = sec;
        this.ns = ns;
    }
    */

    public Ts(long ts) {
        sec = ts / 1000000000L;
        ns = ts % 1000000000L;
    }

    public long sec;
    public long ns;

}
