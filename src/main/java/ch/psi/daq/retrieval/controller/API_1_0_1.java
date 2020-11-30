package ch.psi.daq.retrieval.controller;

import ch.psi.daq.retrieval.*;
import ch.psi.daq.retrieval.config.ConfigurationRetrieval;
import ch.psi.daq.retrieval.finder.BaseDirFinderFormatV0;
import ch.psi.daq.retrieval.pod.api1.channelsearch.ChannelSearchQuery;
import ch.psi.daq.retrieval.pod.api1.Query;
import ch.psi.daq.retrieval.status.RequestStatus;
import ch.qos.logback.classic.Logger;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.context.WebServerInitializedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.core.io.buffer.*;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.ResourceUtils;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.function.Function;

@RestController
public class API_1_0_1 implements ApplicationListener<WebServerInitializedEvent> {
    static Logger LOGGER = (Logger) LoggerFactory.getLogger(API_1_0_1.class);
    @Value("${retrieval.dataBaseDir:UNDEFINED}") public String dataBaseDir;
    @Value("${retrieval.baseKeyspaceName:UNDEFINED}") public String baseKeyspaceName;
    @Value("${retrieval.configFile:UNDEFINED}") String configFile;
    public QueryData queryData;
    public int localPort;
    ConfigurationRetrieval conf;
    InetAddress localAddress = null;
    String localAddressString;
    String localHostname;
    String canonicalHostname;
    static Scheduler dbsched = Schedulers.newParallel("db", 32);
    {
        try {
            localAddress = InetAddress.getLocalHost();
            localHostname = localAddress.getHostName();
            canonicalHostname = localAddress.getCanonicalHostName();
        }
        catch (UnknownHostException e) {
            localHostname = "UNKNOWNHOSTNAME";
        }
        localAddressString = String.format("%s", localAddress);
    }
    DataBufferFactory defaultDataBufferFactory = new DefaultDataBufferFactory();

    @PostMapping(path = "/api/1/query", consumes = MediaType.APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<Flux<DataBuffer>>> query(ServerWebExchange exchange, @RequestBody Mono<Query> queryMono) {
        // The default is octets, to stay compatible with older clients
        ReqCtx reqctx = ReqCtx.fromRequest(exchange.getRequest());
        LOGGER.warn("{}  /query via default endpoint", reqctx);
        if (exchange.getRequest().getHeaders().getAccept().contains(MediaType.APPLICATION_OCTET_STREAM)) {
            LOGGER.warn("{}  started in default endpoint despite having octet-stream set", reqctx);
        }
        return queryProducesOctets(exchange, queryMono);
    }

    // deprecated
    @PostMapping(path = "/api/1.0.1/query", consumes = MediaType.APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<Flux<DataBuffer>>> api101_query(ServerWebExchange exchange, @RequestBody Mono<Query> queryMono) {
        LOGGER.warn("deprecated {}  remote {}", exchange.getRequest().getURI().getPath(), exchange.getRequest().getRemoteAddress());
        return query(exchange, queryMono);
    }

    // deprecated
    @PostMapping(path = "/api/v1/query", consumes = MediaType.APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<Flux<DataBuffer>>> apiv1_query(ServerWebExchange exchange, @RequestBody Mono<Query> queryMono) {
        LOGGER.warn("deprecated {}  remote {}", exchange.getRequest().getURI().getPath(), exchange.getRequest().getRemoteAddress());
        return query(exchange, queryMono);
    }


    @PostMapping(path = "/api/1/query", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public Mono<ResponseEntity<Flux<DataBuffer>>> queryProducesOctets(ServerWebExchange exchange, @RequestBody Mono<Query> queryMono) {
        ReqCtx reqctx = ReqCtx.fromRequest(exchange.getRequest());
        if (conf.mergeLocal) {
            return queryData.queryMergedOctetsLocal(reqctx, exchange, queryMono);
        }
        else {
            return queryData.queryMergedOctets(reqctx, exchange, queryMono);
        }
    }

    // deprecated
    @PostMapping(path = "/api/v1/query", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public Mono<ResponseEntity<Flux<DataBuffer>>> apiv1_queryProducesOctets(ServerWebExchange exchange, @RequestBody Mono<Query> queryMono) {
        LOGGER.warn("deprecated {}  remote {}", exchange.getRequest().getURI().getPath(), exchange.getRequest().getRemoteAddress());
        return queryProducesOctets(exchange, queryMono);
    }

    // deprecated
    @PostMapping(path = "/api/1.0.1/query", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public Mono<ResponseEntity<Flux<DataBuffer>>> api101_queryProducesOctets(ServerWebExchange exchange, @RequestBody Mono<Query> queryMono) {
        LOGGER.warn("deprecated {}  remote {}", exchange.getRequest().getURI().getPath(), exchange.getRequest().getRemoteAddress());
        return queryProducesOctets(exchange, queryMono);
    }


    @PostMapping(path = "/api/1/query", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<Flux<DataBuffer>>> queryProducesJson(ServerWebExchange exchange, @RequestBody Mono<Query> queryMono) {
        ReqCtx reqctx = ReqCtx.fromRequest(exchange.getRequest());
        if (!exchange.getRequest().getHeaders().getAccept().contains(MediaType.APPLICATION_JSON)) {
            LOGGER.warn("{}  /query for json without Accept header", reqctx);
        }
        return queryData.queryMergedJson(reqctx, exchange, queryMono);
    }

    // deprecated
    @PostMapping(path = "/api/1.0.1/query", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<Flux<DataBuffer>>> api101_queryProducesJson(ServerWebExchange exchange, @RequestBody Mono<Query> queryMono) {
        LOGGER.warn("deprecated {}  remote {}", exchange.getRequest().getURI().getPath(), exchange.getRequest().getRemoteAddress());
        return queryProducesJson(exchange, queryMono);
    }

    // deprecated
    @PostMapping(path = "/api/v1/query", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<Flux<DataBuffer>>> apiv1_queryProducesJson(ServerWebExchange exchange, @RequestBody Mono<Query> queryMono) {
        LOGGER.warn("deprecated {}  remote {}", exchange.getRequest().getURI().getPath(), exchange.getRequest().getRemoteAddress());
        return queryProducesJson(exchange, queryMono);
    }


    @PostMapping(path = "/api/1/queryMerged", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public Mono<ResponseEntity<Flux<DataBuffer>>> queryMergedOctets(ServerWebExchange exchange, @RequestBody Mono<Query> queryMono) {
        return queryData.queryMergedOctets(ReqCtx.fromRequest(exchange.getRequest()), exchange, queryMono);
    }

    @PostMapping(path = "/api/1/queryLocal", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public Mono<ResponseEntity<Flux<DataBuffer>>> queryLocal(ServerWebExchange exchange, @RequestBody Mono<Query> queryMono) {
        return queryData.queryLocal(ReqCtx.fromRequest(exchange.getRequest()), exchange, queryMono);
    }

    @PostMapping(path = "/api/1/rng", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public Mono<ResponseEntity<Flux<DataBuffer>>> rng(ServerWebExchange exchange, @RequestBody Mono<Query> queryMono) {
        final int N = 64 * 1024;
        byte[] load = new byte[N];
        DataBufferFactory bufFac = defaultDataBufferFactory;
        Flux<DataBuffer> mret = queryMono
        .doOnError(x -> LOGGER.info("can not parse request"))
        .flatMapMany(query -> Flux.generate(() -> 0L, (st, si) -> {
            byte v = (byte) (0xff & st);
            Arrays.fill(load, v);
            DataBuffer buf = bufFac.allocateBuffer(N);
            buf.write(load);
            si.next(buf);
            return 1 + st;
        }));
        return Mono.just(ResponseEntity.ok().contentType(MediaType.APPLICATION_OCTET_STREAM).body(mret));
    }

    @PostMapping(path = "/api/1/rawLocal", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public Mono<ResponseEntity<Flux<DataBuffer>>> rawLocal(ServerWebExchange exchange, @RequestBody Mono<Query> queryMono) {
        return queryData.rawLocal(ReqCtx.fromRequest(exchange.getRequest(), false), exchange, queryMono);
    }

    @PostMapping(path = "/api/1/queryMergedLocal", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public Mono<ResponseEntity<Flux<DataBuffer>>> queryMergedLocalOctets(ServerWebExchange exchange, @RequestBody Mono<Query> queryMono) {
        return queryData.queryMergedOctetsLocal(ReqCtx.fromRequest(exchange.getRequest()), exchange, queryMono);
    }

    @PostMapping(path = "/api/1/queryJson", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<Flux<DataBuffer>>> queryJson(ServerWebExchange exchange, @RequestBody Mono<Query> queryMono) {
        return queryMergedJson(exchange, queryMono);
    }

    @PostMapping(path = "/api/1/queryMergedJson", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<Flux<DataBuffer>>> queryMergedJson(ServerWebExchange exchange, @RequestBody Mono<Query> queryMono) {
        return queryData.queryMergedJson(ReqCtx.fromRequest(exchange.getRequest()), exchange, queryMono);
    }

    Flux<DataBuffer> channelsJson(DataBufferFactory bufFac, ChannelSearchQuery q, boolean configOut) {
        return Flux.generate(() -> ChannelLister.create(conf, bufFac, q.order(), q.regex, q.sourceRegex, q.descriptionRegex, configOut), ChannelLister::generate, ChannelLister::release)
        .subscribeOn(dbsched)
        .flatMapIterable(Function.identity());
    }

    @GetMapping(path = "/api/1/channels", produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<Flux<DataBuffer>>> channelsGet(ServerWebExchange exchange) {
        ReqCtx reqctx = ReqCtx.fromRequest(exchange.getRequest());
        LOGGER.debug("{}  request for channelsGet", reqctx);
        ChannelSearchQuery q = new ChannelSearchQuery();
        q.ordering = "asc";
        return Mono.just(channelsJson(exchange.getResponse().bufferFactory(), q, false))
        .map(fl -> {
            LOGGER.info("{}  building response entity", reqctx);
            return ResponseEntity.ok()
            .header("X-CanonicalHostname", canonicalHostname)
            .contentType(MediaType.APPLICATION_JSON)
            .body(fl);
        });
    }

    @GetMapping(path = "/api/1/channels/search/regexp/{regexp}", produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<Flux<DataBuffer>>> channelsGet(ServerWebExchange exchange, @PathVariable String regexp) {
        ReqCtx reqctx = ReqCtx.fromRequest(exchange.getRequest());
        LOGGER.info("{}  request for channelsRegexp  [{}]", reqctx, regexp);
        ChannelSearchQuery q = new ChannelSearchQuery();
        q.ordering = "asc";
        return Mono.just(channelsJson(exchange.getResponse().bufferFactory(), q, false))
        .map(fl -> {
            return ResponseEntity.ok()
            .header("X-CanonicalHostname", canonicalHostname)
            .contentType(MediaType.APPLICATION_JSON)
            .body(fl);
        });
    }

    @PostMapping(path = "/api/1/channels", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<Flux<DataBuffer>>> channelsPost(ServerWebExchange exchange, @RequestBody Mono<ChannelSearchQuery> queryMono) {
        ReqCtx reqctx = ReqCtx.fromRequest(exchange.getRequest());
        LOGGER.debug("{}  request for channelsPost", reqctx);
        return queryMono.map(query -> {
            if (!query.valid()) {
                throw new RuntimeException("invalid query");
            }
            LOGGER.debug("{}  regex: {}", reqctx, query.regex);
            return channelsJson(exchange.getResponse().bufferFactory(), query, false);
        })
        .map(fl -> {
            return ResponseEntity.ok()
            .header("X-CanonicalHostname", canonicalHostname)
            .contentType(MediaType.APPLICATION_JSON)
            .body(fl);
        });
    }

    @PostMapping(path = "/api/1/channels/config", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<Flux<DataBuffer>>> channelsConfigPost(ServerWebExchange exchange, @RequestBody Mono<ChannelSearchQuery> queryMono) {
        ReqCtx reqctx = ReqCtx.fromRequest(exchange.getRequest());
        LOGGER.info("{}  request for channelsPost", reqctx);
        return queryMono.map(query -> {
            if (!query.valid()) {
                throw new RuntimeException("invalid query");
            }
            LOGGER.debug("{}  regex: {}", reqctx, query.regex);
            return channelsJson(exchange.getResponse().bufferFactory(), query, true);
        })
        .map(fl -> {
            return ResponseEntity.ok()
            .header("X-CanonicalHostname", canonicalHostname)
            .contentType(MediaType.APPLICATION_JSON)
            .body(fl);
        });
    }

    @GetMapping(path = "/api/1/paramsList/{params}", produces = MediaType.APPLICATION_JSON_VALUE)
    public String channelsGet(@PathVariable List<String> params) {
        return String.format("len %d  %s", params.size(), params.toString());
    }

    @GetMapping(path = "/api/1/paramsMap/{params}", produces = MediaType.APPLICATION_JSON_VALUE)
    public String channelsGet(@PathVariable Map<String, String> params) {
        return String.format("len %d  %s", params.size(), params.toString());
    }

    public static void logHeaders(ServerWebExchange ex) {
        for (String n : List.of("User-Agent", "X-PythonDataAPIPackageVersion", "X-PythonDataAPIModule")) {
            LOGGER.info("{}  header {} {}", ReqCtx.fromRequest(ex.getRequest()), n, ex.getRequest().getHeaders().get(n));
        }
    }

    @GetMapping(path = "/api/1/requestStatus/{reqid}", produces = MediaType.APPLICATION_JSON_VALUE)
    public RequestStatus requestStatus(ServerWebExchange ex, @PathVariable String reqid) {
        LOGGER.debug("{}  requestStatus  reqid {}", ReqCtx.fromRequest(ex.getRequest()), reqid);
        return queryData.requestStatusBoard().get(reqid);
    }

    // deprecated
    @GetMapping(path = "/api/1.0.1/requestStatus/{reqid}", produces = MediaType.APPLICATION_JSON_VALUE)
    public RequestStatus api101_requestStatus(ServerWebExchange ex, @PathVariable String reqid) {
        LOGGER.warn("deprecated {}  remote {}", ex.getRequest().getURI().getPath(), ex.getRequest().getRemoteAddress());
        return requestStatus(ex, reqid);
    }

    // deprecated
    @GetMapping(path = "/api/v1/requestStatus/{reqid}", produces = MediaType.APPLICATION_JSON_VALUE)
    public RequestStatus apiv1_requestStatus(ServerWebExchange ex, @PathVariable String reqid) {
        LOGGER.warn("deprecated {}  remote {}", ex.getRequest().getURI().getPath(), ex.getRequest().getRemoteAddress());
        return requestStatus(ex, reqid);
    }


    @PostMapping(path = "/api/1/channels_timeout", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<String> channelSearchtimeout(@RequestBody Mono<ChannelSearchQuery> queryMono) {
        return Mono.just("").delayElement(Duration.ofSeconds(10));
    }

    @PostMapping(path = "/api/1/channels_error500", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<ByteBuffer> channelSearchError500(@RequestBody Mono<ChannelSearchQuery> queryMono) {
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
    }

    ConfigurationRetrieval loadConfiguration(File f1) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new JsonFactory());
        ConfigurationRetrieval conf = mapper.readValue(f1, ConfigurationRetrieval.class);
        if (conf.splitNodes != null) {
            for (SplitNode sn : conf.splitNodes) {
                if (sn.host == null) {
                    sn.host = "localhost";
                }
                if (sn.port == 0) {
                    sn.port = localPort;
                }
            }
        }
        return conf;
    }

    ConfigurationRetrieval loadConfiguration(WebServerInitializedEvent ev) throws IOException {
        if (configFile != null && !configFile.equals("UNDEFINED")) {
            LOGGER.info("try file: {}", configFile);
            File f1 = ResourceUtils.getFile(configFile);
            LOGGER.info("load from: {}", f1);
            return loadConfiguration(f1);
        }
        else {
            try {
                File f1 = ResourceUtils.getFile("classpath:retrieval.json");
                LOGGER.info("load from: {}", f1);
                return loadConfiguration(f1);
            }
            catch (Exception e) {
                LOGGER.info("no default configFile found.");
            }
        }
        return null;
    }

    @Override
    public void onApplicationEvent(WebServerInitializedEvent ev) {
        List<SplitNode> splitNodes = List.of();
        localPort = ev.getWebServer().getPort();
        try {
            ConfigurationRetrieval conf = loadConfiguration(ev);
            conf.validate();
            LOGGER.info("loaded: {}", conf);
            if (conf != null) {
                this.conf = conf;
                splitNodes = conf.splitNodes;
            }
        }
        catch (ConfigurationRetrieval.InvalidException e) {
            LOGGER.error("Invalid configuration: {}", e.toString());
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        Hooks.onNextDropped(obj -> {
            LOGGER.error("Hooks.onNextDropped  {}", obj);
        });
        Hooks.onOperatorError((err, obj) -> {
            LOGGER.error("Hooks.onOperatorError  {}", obj);
            return err;
        });
        LOGGER.info("localPort {}  dataBaseDir {}", localPort, dataBaseDir);
        queryData = new QueryData(new BaseDirFinderFormatV0(Path.of(dataBaseDir), baseKeyspaceName), splitNodes, canonicalHostname);
    }

    public long getTotalBytesServed() {
        return queryData.totalBytesServed.get();
    }

}
