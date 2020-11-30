package ch.psi.daq.retrieval.pod.api1;

import com.fasterxml.jackson.annotation.JsonProperty;

public enum Order {
    @JsonProperty("asc")
    ASC,
    @JsonProperty("desc")
    DESC,
}
