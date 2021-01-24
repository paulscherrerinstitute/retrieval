package ch.psi.daq.retrieval.pod.api1;

import ch.qos.logback.classic.Level;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(value = JsonInclude.Include.NON_DEFAULT)
public class Query {
    public List<String> channels;
    public Range range;
    public Aggregation aggregation;
    // The following are for testing usage only:
    public List<Integer> splits;
    public int bufferSize;
    public int decompressOnServer;
    public long limitEventsPerChannel;
    public long limitBytes;
    public long limitBytesPerChannel;
    public int errorAfterBytes;
    public int queryMergedLocalVariant;
    public long throttleRate;
    public int throttleSteps;
    public int throttleInterval;
    public int throttleOverslack;
    public int subreqType;
    public int valuemapType;
    public int tsmapType;
    public int mergeType;
    public int prepareSubfluxPrefetch;
    public int subTokenRate;
    public int flattenSlicesPrefetch;
    public int mergerSupportItemVecPrefetch;
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
