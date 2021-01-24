package ch.psi.daq.retrieval.status;

import ch.psi.daq.retrieval.ReqCtx;
import ch.qos.logback.classic.Logger;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

@JsonInclude(value = JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class RequestStatus {
    static final Logger LOGGER = (Logger) LoggerFactory.getLogger(RequestStatus.class.getSimpleName());
    public static class Error {
        public Error(String msg) {
            this.msg = msg;
        }
        public String msg;
    }

    static final DateTimeFormatter dtfmt = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSz");
    static final ZoneId zulu = ZoneId.of("Z");

    public RequestStatus() {}
    public RequestStatus(ReqCtx reqCtx) {
        this.reqCtx = reqCtx;
    }

    public ReqCtx reqCtx;
    public List<Error> errors;
    public List<RequestStatus> subRequestStatuses;

    ZonedDateTime tsl = ZonedDateTime.now(zulu);

    @JsonProperty("tsl")
    String tsl() {
        return tsl.withZoneSameInstant(zulu).format(dtfmt);
    }

    @JsonProperty("tsl")
    void setTsl(String k) {
        tsl = ZonedDateTime.parse(k, dtfmt);
    }

    public void ping() {
        tsl = ZonedDateTime.now(zulu);
    }

    public void addError(Error err) {
        if (errors == null) {
            errors = new ArrayList<>();
        }
        errors.add(err);
    }

    public void addSubRequestStatus(RequestStatus status) {
        if (status.errors != null) {
            LOGGER.info("{}  addSubRequestStatus  add {} errors", reqCtx, status.errors.size());
            status.errors.forEach(this::addError);
        }
    }

    public String summary() {
        try {
            return new ObjectMapper().writeValueAsString(this);
        }
        catch (IOException e) {
            return "ERROR JACKSON " + e.toString();
        }
    }

    public RequestStatus clone2() {
        RequestStatus ret = new RequestStatus();
        if (this.tsl != null) {
            ret.tsl = this.tsl.plusNanos(0);
            if (ret.tsl == this.tsl) {
                throw new RuntimeException("bad tsl clone");
            }
        }
        if (this.reqCtx != null) {
            ret.reqCtx = this.reqCtx.clone2();
        }
        if (this.subRequestStatuses != null) {
            ret.subRequestStatuses = new ArrayList<>();
            for (RequestStatus k : this.subRequestStatuses) {
                ret.subRequestStatuses.add(k.clone2());
            }
        }
        return ret;
    }

}
