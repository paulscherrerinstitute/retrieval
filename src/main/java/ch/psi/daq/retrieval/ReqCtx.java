package ch.psi.daq.retrieval;

import ch.psi.daq.retrieval.finder.Channel;
import ch.qos.logback.classic.Level;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.server.ServerWebExchange;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class ReqCtx {
    @JsonIgnore
    public Level logLevel = Level.INFO;
    @JsonIgnore
    public BufCtx bufCtx;
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
    final Set<String> channelsWithFilesNotFound = new HashSet<>();
    @JsonProperty
    AtomicLong channelFilesFound = new AtomicLong();
    @JsonProperty
    AtomicLong channelFilesNotFound = new AtomicLong();
    @JsonProperty
    AtomicLong configParseDurationTotalMu = new AtomicLong();
    private static final int BUFFER_SIZE = 1024 * 32;

    public static ReqCtx fromRequest(ServerWebExchange exchange) {
        return fromRequest(exchange, true);
    }

    public static ReqCtx fromRequest(ServerWebExchange exchange, boolean doAgent) {
        ServerHttpRequest req = exchange.getRequest();
        ReqCtx ret = new ReqCtx(exchange.getResponse().bufferFactory(), BUFFER_SIZE, req.getId());
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
    public static ReqCtx forTestWithBufferSize(int bufferSize) {
        return new ReqCtx(new DefaultDataBufferFactory(), bufferSize, "test");
    }
    public static ReqCtx dummy() {
        ReqCtx ret = new ReqCtx(DefaultDataBufferFactory.sharedInstance, BUFFER_SIZE, "dummy");
        ret.scheme = "tcp";
        // TODO
        ret.host = "unknown";
        return ret;
    }

    public ReqCtx() {
    }

    ReqCtx(DataBufferFactory buffac, int bufferSize, String reqId) {
        this.reqId = reqId;
        bufCtx = new BufCtx(buffac, bufferSize);
    }

    @Override
    public String toString() { return reqId; }

    public void addBodyLen(int k) {
        responseBodyLen.getAndAdd(k);
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

    public ReqCtx clone2() {
        ReqCtx ret = new ReqCtx(bufCtx.bufFac, bufCtx.bufferSize, reqId);
        ret.beginToBodyEmittedMu = this.beginToBodyEmittedMu;
        if (this.host != null) {
            ret.host = this.host + "";
            if (ret.host == this.host) {
                throw new RuntimeException("bad host clone");
            }
        }
        if (this.reqId != null) {
            ret.reqId = this.reqId + "";
            if (ret.reqId == this.reqId) {
                throw new RuntimeException("bad reqId clone");
            }
        }
        if (this.mainReqId != null) {
            ret.mainReqId = this.mainReqId + "";
            if (ret.mainReqId == this.mainReqId) {
                throw new RuntimeException("bad mainReqId clone");
            }
        }
        if (this.path != null) {
            ret.path = this.path + "";
            if (ret.path == this.path) {
                throw new RuntimeException("bad path clone");
            }
        }
        if (this.scheme != null) {
            ret.scheme = this.scheme + "";
            if (ret.scheme == this.scheme) {
                throw new RuntimeException("bad scheme clone");
            }
        }
        if (this.UserAgent != null) {
            ret.UserAgent = new ArrayList<>();
            for (String k : this.UserAgent) {
                ret.UserAgent.add(k + "");
            }
        }
        return ret;
    }

}
