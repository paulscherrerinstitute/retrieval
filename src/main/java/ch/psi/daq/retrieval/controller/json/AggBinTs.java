package ch.psi.daq.retrieval.controller.json;

import ch.psi.daq.retrieval.pod.api1.Ts;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;

public class AggBinTs {

    public AggBinTs(long begTs, long endTs) {
        startTs = new Ts(begTs);
        this.endTs = new Ts(endTs);
    }

    public Ts startTs;
    public Ts endTs;
    public List<JsonNode> results;

}
