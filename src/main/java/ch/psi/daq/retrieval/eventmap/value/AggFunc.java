package ch.psi.daq.retrieval.eventmap.value;

import ch.psi.daq.retrieval.pod.api1.AggResult;
import com.fasterxml.jackson.databind.JsonNode;

public interface AggFunc {
    String name();
    void sink(JsonNode node);
    void reset();
    AggResult result();
}
