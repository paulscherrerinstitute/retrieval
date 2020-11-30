package ch.psi.daq.retrieval.status;

import ch.psi.daq.retrieval.ReqCtx;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

@JsonInclude(value = JsonInclude.Include.NON_NULL)
public class RequestStatus {
    public static class Error {
        public Error(String msg) {
            this.msg = msg;
        }

        public String msg;
    }

    static final DateTimeFormatter dtfmt = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSz");
    static final ZoneId zulu = ZoneId.of("Z");

    public RequestStatus() {}
    public RequestStatus(ReqCtx reqctx) {
        this.reqctx = reqctx;
    }

    public ReqCtx reqctx;
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
        if (subRequestStatuses == null) {
            subRequestStatuses = new ArrayList<>();
        }
        subRequestStatuses.add(status);
    }

    public String summary() {
        try {
            return new ObjectMapper().writeValueAsString(this);
        }
        catch (IOException e) {
            return "ERROR JACKSON " + e.toString();
        }
    }

}
