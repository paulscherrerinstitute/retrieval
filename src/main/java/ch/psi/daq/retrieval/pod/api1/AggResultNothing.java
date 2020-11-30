package ch.psi.daq.retrieval.pod.api1;

import com.fasterxml.jackson.annotation.JsonValue;

public class AggResultNothing implements AggResult {
    @JsonValue
    public String value;
}
