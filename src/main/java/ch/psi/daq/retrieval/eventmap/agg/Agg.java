package ch.psi.daq.retrieval.eventmap.agg;

import com.fasterxml.jackson.databind.JsonNode;

public interface Agg {

    String name();
    JsonNode toJsonNode();
    void clear();

}
