package ch.psi.daq.retrieval;

import ch.psi.daq.retrieval.finder.Channel;
import ch.qos.logback.classic.Level;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.server.reactive.ServerHttpRequest;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class ReqCtx {
    @JsonIgnore
    public Level logLevel;
    @JsonProperty("logLevel")
    public String logLevel() { return logLevel.levelStr; }
    @JsonProperty("logLevel")
    public void setLogLevel(String level) { logLevel = Level.toLevel(level); }
    public String reqId;
    public String scheme;
    public String host;
    public int port;
    public String path;
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public List<String> UserAgent;
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public List<String> PythonDataAPIPackageVersion;
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public List<String> PythonDataAPIModule;
    @JsonInclude(value = JsonInclude.Include.NON_NULL)
    public String mainReqId;
    long tsInit = System.nanoTime();
    public long beginToBodyEmittedMu;
    public AtomicLong responseBodyLen = new AtomicLong();
    Set<String> channelsWithFilesNotFound = new HashSet<>();
    @JsonProperty
    AtomicLong channelFilesFound = new AtomicLong();
    @JsonProperty
    AtomicLong channelFilesNotFound = new AtomicLong();
    @JsonProperty
    AtomicLong configParseDurationTotalMu = new AtomicLong();
    public static ReqCtx fromRequest(ServerHttpRequest req) {
        return fromRequest(req, true);
    }
    public static ReqCtx fromRequest(ServerHttpRequest req, boolean doAgent) {
        ReqCtx ret = new ReqCtx(req.getId());
        ret.scheme = req.getURI().getScheme();
        ret.host = req.getURI().getHost();
        ret.port = req.getURI().getPort();
        ret.path = req.getURI().getPath();
        if (doAgent) {
            ret.UserAgent = req.getHeaders().get("User-Agent");
            ret.PythonDataAPIPackageVersion = req.getHeaders().get("X-PythonDataAPIPackageVersion");
            ret.PythonDataAPIModule = req.getHeaders().get("X-PythonDataAPIModule");
        }
        List<String> ll = req.getHeaders().get("X-LogLevel");
        if (ll != null) {
            if (ll.contains("debug")) {
                ret.logLevel = Level.DEBUG;
            }
            else if (ll.contains("trace")) {
                ret.logLevel = Level.TRACE;
            }
        }
        return ret;
    }
    public static ReqCtx dummy() {
        return new ReqCtx("noReqId");
    }
    ReqCtx() {}
    ReqCtx(String reqId) {
        this.logLevel = Level.INFO;
        this.reqId = reqId;
    }
    @Override
    public String toString() { return reqId; }
    public void addBodyLen(DataBuffer buf) {
        responseBodyLen.getAndAdd(buf.readableByteCount());
    }
    public void channelFileFound(Channel channel, Path path) {
        channelFilesFound.getAndAdd(1);
    }
    public void channelFileNotFound(Channel channel, Path path) {
        channelFilesNotFound.getAndAdd(1);
        synchronized (channelsWithFilesNotFound) {
            channelsWithFilesNotFound.add(channel.name);
        }
    }
    public void addConfigParseDuration(long nanos) {
        configParseDurationTotalMu.getAndAdd(nanos / 1000);
    }
    public void bodyEmitted() {
        beginToBodyEmittedMu = (System.nanoTime() - tsInit) / 1000;
    }
}
