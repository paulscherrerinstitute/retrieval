package ch.psi.daq.retrieval.pod.api1;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@JsonSerialize(using = ChannelListSerializer.class)
@JsonDeserialize(using = ChannelListDeserializer.class)
public class ChannelList {

    public ChannelList(List<?> list) {
        if (list.size() > 0) {
            if ((list.get(0)) instanceof Channel) {
                channels = list.stream().map(k -> (Channel) k).collect(Collectors.toList());
            }
            else if ((list.get(0)) instanceof String) {
                channels = list.stream().map(k -> new Channel(null, (String) k)).collect(Collectors.toList());
            }
        }
        else {
            channels = new ArrayList<>();
        }
    }

    @Override
    public String toString() {
        try {
            ObjectMapper om = new ObjectMapper();
            return om.writeValueAsString(this);
        }
        catch (JsonProcessingException e) {
            return "[formatting error " + e.toString() + "]";
        }
    }

    public List<Channel> channels;

}
