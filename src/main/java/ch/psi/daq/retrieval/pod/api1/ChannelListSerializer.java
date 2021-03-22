package ch.psi.daq.retrieval.pod.api1;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

public class ChannelListSerializer extends StdSerializer<ChannelList> {

    public ChannelListSerializer() {
        super(ChannelList.class);
    }

    @Override
    public void serialize(ChannelList list, JsonGenerator jgen, SerializerProvider prov) throws IOException {
        jgen.writeStartArray();
        for (Channel c : list.channels) {
            if (c.backend == null) {
                jgen.writeString(c.name);
            }
            else {
                jgen.writeStartObject();
                jgen.writeStringField("backend", c.backend);
                jgen.writeStringField("name", c.name);
                jgen.writeEndObject();
            }
        }
        jgen.writeEndArray();
    }

}
