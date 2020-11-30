package ch.psi.daq.retrieval.eventmap.value;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;

public class MapJsonEvent implements MapJsonItem {
    public long ts;
    public long pulse;
    public JsonNode data;

    @Override
    public void release() {
    }

    public void mapOutput(AggMapper mapper, JsonGenerator jgen, JgenState jst) {
        mapper.processEvent(this);
    }

}
