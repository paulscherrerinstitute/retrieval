package ch.psi.daq.retrieval.controller;

import ch.psi.daq.retrieval.ChannelEventStream;
import ch.psi.daq.retrieval.bytes.BufCont;
import ch.psi.daq.retrieval.eventmap.ts.EventBlobToV1MapTs;
import ch.psi.daq.retrieval.eventmap.ts.MapTsItemVec;
import ch.psi.daq.retrieval.eventmap.value.EventBlobToV1Map;
import ch.psi.daq.retrieval.merger.Merger;
import ch.psi.daq.retrieval.status.RequestStatusBoard;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class Overview {
    public ZonedDateTime ts;
    public String version;
    public String buildTime;
    public String commitDescribe;
    public String configFile;
    public long totalBytesEmitted = QueryData.totalBytesEmitted.get();
    public int statusMapCount;
    @JsonProperty
    String getTsStartup() {
        return API_1_0_1.tsStartup.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssz"));
    }
    public long totalDataRequests;
    public JsonNode info;
    public String showTest;
    @JsonProperty
    public BufCont.Stats getBufContStats() {
        return new BufCont.Stats();
    }
    @JsonProperty
    public EventBlobToV1MapTs.Stats getEventBlobToV1MapTsStats() {
        return new EventBlobToV1MapTs.Stats();
    }
    @JsonProperty
    public EventBlobToV1Map.Stats getEventBlobToV1MapStats() {
        return new EventBlobToV1Map.Stats();
    }
    @JsonProperty
    public MapTsItemVec.FluxIterStats getMapTsItemVecFluxIterStats() {
        return new MapTsItemVec.FluxIterStats();
    }
    @JsonProperty
    public ChannelEventStream.Stats getChannelEventStreamStats() {
        return new ChannelEventStream.Stats();
    }
    @JsonProperty
    public Merger.Stats getMergerStats() {
        return new Merger.Stats();
    }
    @JsonProperty
    public QueryData.Stats getQueryDataStats() {
        return new QueryData.Stats();
    }
    @JsonProperty
    public RawSub.Stats getRawSubStats() {
        return new RawSub.Stats();
    }
    @JsonProperty
    public RawClient.Stats getRawClientStats() {
        return new RawClient.Stats();
    }
    @JsonProperty
    String getDetectionLevel() { return System.getProperty("io.netty.leakDetection.level"); }
    @JsonProperty
    String getDetectionTargetRecords() { return System.getProperty("io.netty.leakDetection.targetRecords"); }
    public RequestStatusBoard.Stats requestStatusBoardStats;
}
