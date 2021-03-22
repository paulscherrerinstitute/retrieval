package ch.psi.daq.retrieval.status;

import ch.psi.daq.retrieval.reqctx.ReqCtx;
import ch.psi.daq.retrieval.utils.DateExt;
import ch.qos.logback.classic.Logger;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@JsonInclude(value = JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class RequestStatus {
    static final Logger LOGGER = (Logger) LoggerFactory.getLogger(RequestStatus.class.getSimpleName());
    static final ZoneOffset utc = ZoneOffset.UTC;

    public RequestStatus() {}

    public RequestStatus(ReqCtx reqCtx) {
        this.reqCtx = Objects.requireNonNull(reqCtx);
        host = reqCtx.host;
        port = reqCtx.port;
    }

    @JsonProperty("tsl")
    String tsl() {
        return tsl.withZoneSameInstant(utc).format(DateExt.datefmt);
    }

    @JsonProperty("tsl")
    void setTsl(String k) {
        tsl = ZonedDateTime.parse(k, DateExt.datefmt);
    }

    @JsonProperty
    public int bufCapMax() {
        return bufCapMax;
    }

    public void ping() {
        tsl = ZonedDateTime.now(utc);
    }

    public void addError(Error err) {
        if (errors == null) {
            errors = new ArrayList<>();
        }
        errors.add(err);
    }

    public synchronized void addSubRequestStatus(RequestStatus status) {
        if (status.errors != null) {
            String from = status.reqCtx.host + ":" + status.reqCtx.port;
            LOGGER.error("{}  addSubRequestStatus  sees {} errors from {}", reqCtx, status.errors.size(), from);
            status.errors.forEach(k -> {
                LOGGER.error("{}  addSubRequestStatus  {}  error: {}", reqCtx, from, k.msg);
                addError(k);
            });
        }
        if (status.bufCapMax() > 0) {
            bufCapMax = Math.max(bufCapMax, status.bufCapMax());
        }
    }

    public String summary() {
        try {
            return new ObjectMapper().writeValueAsString(this);
        }
        catch (IOException e) {
            return "[jackson error: " + e.toString() + "]";
        }
    }

    public synchronized void seenBufCapMax(int cap) {
        bufCapMax = Math.max(cap, bufCapMax);
    }

    public String host;
    public int port;
    public List<Error> errors;
    ZonedDateTime tsl = ZonedDateTime.now(utc);
    int bufCapMax;
    //@JsonIgnore
    public ReqCtx reqCtx;
    public List<BufStatsCollector> bufStatsCollectors = new ArrayList<>();

}
