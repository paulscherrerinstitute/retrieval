package ch.psi.daq.retrieval.pod.api1;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ChannelListDeserializer extends StdDeserializer<ChannelList> {

    public ChannelListDeserializer() {
        super(ChannelList.class);
        this.defaultBackend = null;
    }

    @Override
    public ChannelList deserialize(JsonParser jp, DeserializationContext ctx) throws IOException {
        ArrayNode arr = jp.readValueAsTree();
        List<Channel> list = new ArrayList<>();
        for (JsonNode n : arr) {
            if (n.isTextual()) {
                String s = n.asText();
                if (s.contains("/")) {
                    String[] parts = s.split("/");
                    if (parts.length != 2) {
                        throw new RuntimeException("unexpected channel list format");
                    }
                    else {
                        list.add(new Channel(parts[0], parts[1]));
                    }
                }
                else {
                    list.add(new Channel(defaultBackend, s));
                }
            }
            else if (n.isObject()) {
                try {
                    list.add(new Channel(n.get("backend").asText(), n.get("name").asText()));
                }
                catch (Exception e) {
                    throw new RuntimeException("unexpected channel list format");
                }
            }
            else {
                throw new RuntimeException("unexpected channel list format");
            }
        }
        return new ChannelList(list);
    }

    public ChannelList deserializeStringList(JsonParser jp, DeserializationContext ctx) throws IOException {
        @SuppressWarnings("unchecked")
        List<JsonNode> plainList = jp.readValueAs(List.class);
        List<Channel> list = new ArrayList<>();
        for (JsonNode n : plainList) {
            if (n.isTextual()) {
                list.add(new Channel(defaultBackend, n.asText()));
            }
        }
        return new ChannelList(list);
    }

    String defaultBackend;

}
