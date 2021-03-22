package ch.psi.daq.retrieval.reqctx;

import ch.psi.daq.retrieval.config.ConfigurationRetrieval;
import ch.psi.daq.retrieval.finder.LocatedChannel;
import ch.psi.daq.retrieval.status.RequestStatus;
import ch.psi.daq.retrieval.status.RequestStatusBoard;
import ch.qos.logback.classic.Level;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.io.BaseEncoding;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.server.ServerWebExchange;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
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

    public static ReqCtx fromRequest(ServerWebExchange exchange, RequestStatusBoard rsb) {
        return fromRequest(exchange, true, rsb);
    }

    public static ReqCtx fromRequest(ServerWebExchange exchange, boolean doAgent, RequestStatusBoard rsb) {
        ServerHttpRequest req = exchange.getRequest();
        ReqCtx ret = new ReqCtx(exchange.getResponse().bufferFactory(), req.getId(), rsb);
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

    public static ReqCtx fromRawRequest(RequestStatusBoard rsb) {
        ReqCtx ret = new ReqCtx(DefaultDataBufferFactory.sharedInstance, createReqId(), rsb);
        ret.scheme = "tcp";
        return ret;
    }

    public static ReqCtx forTestWithBufferSize(int bufferSize) {
        if (true) throw new RuntimeException("todo adapt");
        return new ReqCtx(new DefaultDataBufferFactory(), "test", new RequestStatusBoard(new ConfigurationRetrieval()));
    }

    public static ReqCtx dummy() {
        if (true) throw new RuntimeException("todo rsb not avail");
        ReqCtx ret = new ReqCtx(DefaultDataBufferFactory.sharedInstance, "dummy", null);
        ret.scheme = "tcp";
        // TODO
        ret.host = "unknown";
        return ret;
    }

    public ReqCtx() {
    }

    ReqCtx(DataBufferFactory buffac, String reqId, RequestStatusBoard rsb) {
        this.reqId = reqId;
        this.requestStatus = rsb.getOrCreate(this);
        bufCtx = new BufCtx(buffac);
    }

    @Override
    public String toString() { return reqId; }

    public void addBodyLen(int k) {
        responseBodyLen.getAndAdd(k);
    }

    public void channelFileFound(LocatedChannel locatedChannel, Path path) {
        channelFilesFound.getAndAdd(1);
    }

    public void channelFileNotFound(LocatedChannel locatedChannel, Path path) {
        channelFilesNotFound.getAndAdd(1);
        synchronized (channelsWithFilesNotFound) {
            channelsWithFilesNotFound.add(locatedChannel.name);
        }
    }

    public void addConfigParseDuration(long nanos) {
        configParseDurationTotalMu.getAndAdd(nanos / 1000);
    }

    public void bodyEmitted() {
        beginToBodyEmittedMu = (System.nanoTime() - tsInit) / 1000;
    }

    public RequestStatus requestStatus() {
        return requestStatus;
    }

    static String createReqId() {
        synchronized (reqIdRng) {
            byte[] buf = new byte[8];
            reqIdRng.nextBytes(buf);
            return "RawSub-" + BaseEncoding.base32Hex().lowerCase().omitPadding().encode(buf);
        }
    }

    RequestStatus requestStatus;
    static final Random reqIdRng = new Random();

}
