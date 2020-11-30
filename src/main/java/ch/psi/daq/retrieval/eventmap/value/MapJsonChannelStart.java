package ch.psi.daq.retrieval.eventmap.value;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.List;

public class MapJsonChannelStart implements MapJsonItem {
    public String name;
    public String type;
    public ByteOrder byteOrder;
    public List<Integer> shape;
    public boolean array;

    @Override
    public void release() {
    }

    public void mapOutput(AggMapper mapper, JsonGenerator jgen, JgenState jst) throws IOException {
        jst.beOutOfChannel(jgen);
        jst.inChannel = true;
        jgen.writeStartObject();
        jgen.writeStringField("name", name);
        jgen.writeFieldName("data");
        jgen.writeStartArray();
    }

}
