package ch.psi.daq.retrieval.eventmap.value;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;

public interface MapJsonItem {
    void release();
    void mapOutput(AggMapper mapper, JsonGenerator jgen, JgenState jst) throws IOException;
}
