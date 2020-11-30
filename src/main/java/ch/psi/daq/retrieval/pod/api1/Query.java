package ch.psi.daq.retrieval.pod.api1;

import ch.qos.logback.classic.Level;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSetter;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Query {
    public List<String> channels;
    public Range range;
    public Aggregation aggregation;
    // The following are for testing usage only:
    public List<Integer> splits;
    public int bufferSize;
    public int decompressOnServer;
    public long limitBytes;
    public int errorAfterBytes;
    @JsonSetter
    public void setLoglevel(String name) {
        if (name.equals("trace")) {
            loglevel = Level.TRACE;
        }
        else if (name.equals("debug")) {
            loglevel = Level.DEBUG;
        }
        else if (name.equals("info")) {
            loglevel = Level.INFO;
        }
        else if (name.equals("warn")) {
            loglevel = Level.WARN;
        }
        else if (name.equals("error")) {
            loglevel = Level.ERROR;
        }
        else {
            loglevel = Level.WARN;
        }
    }
    private Level loglevel = Level.INFO;
    public Level logLevel() {
        return loglevel;
    }
}
