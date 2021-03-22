package ch.psi.daq.retrieval.controller;

import ch.psi.daq.retrieval.ChannelLister;
import ch.psi.daq.retrieval.reqctx.ReqCtx;
import ch.psi.daq.retrieval.bytes.BufCont;
import ch.psi.daq.retrieval.config.ConfigurationRetrieval;
import ch.psi.daq.retrieval.finder.BaseDirFinderFormatV0;
import ch.psi.daq.retrieval.pod.api1.Query;
import ch.psi.daq.retrieval.pod.api1.channelsearch.ChannelSearchQuery;
import ch.psi.daq.retrieval.status.RequestStatus;
import ch.psi.daq.retrieval.status.RequestStatusBoard;
import ch.psi.daq.retrieval.subnodes.RawSub;
import ch.psi.daq.retrieval.throttle.Throttle;
import ch.qos.logback.classic.Logger;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.context.WebServerInitializedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.util.ResourceUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import javax.annotation.PreDestroy;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

@RestController
public class API_1_0_1 implements ApplicationListener<WebServerInitializedEvent>, DisposableBean {
    static Logger LOGGER = (Logger) LoggerFactory.getLogger(API_1_0_1.class.getSimpleName());
    @Value("${retrieval.configFile:#{null}}") String configFile;
    public QueryData queryData;
    public RequestStatusBoard rsb;
    public int localPort;
    public ConfigurationRetrieval conf;
    InetAddress localAddress;
    String localAddressString;
    String localHostname;
    String canonicalHostname;
    static Scheduler dbsched = Schedulers.newBoundedElastic(32, 256, "db");
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
    public static final ZonedDateTime tsStartup = ZonedDateTime.now(ZoneOffset.UTC);
    final AtomicLong totalDataRequests = new AtomicLong();

    @Scheduled(fixedRate = 4000)
    public void statusgc() {
        if (queryData != null) {
            queryData.scheduledStatusClean();
        }
        else {
            LOGGER.warn("statusgc queryData not yet ready");
        }
    }

    @Scheduled(fixedRate = 10000)
    public void buffergc() {
        BufCont.gc();
    }

    @PostMapping(path = "/api/1/query", consumes = MediaType.APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<Flux<DataBuffer>>> query(ServerWebExchange exchange, @RequestBody Mono<Query> queryMono) {
        // The default is octets, to stay compatible with older clients
        ReqCtx reqctx = ReqCtx.fromRequest(exchange, rsb);
        LOGGER.warn("{}  /query via default endpoint", reqctx);
        if (exchange.getRequest().getHeaders().getAccept().contains(MediaType.APPLICATION_OCTET_STREAM)) {
            LOGGER.warn("{}  started in default endpoint despite having octet-stream set", reqctx);
        }
        return queryProducesOctets(exchange, queryMono);
    }


    @PostMapping(path = "/api/1/query", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public Mono<ResponseEntity<Flux<DataBuffer>>> queryProducesOctets(ServerWebExchange exchange, @RequestBody Mono<Query> queryMono) {
        totalDataRequests.getAndAdd(1);
        ReqCtx reqctx = ReqCtx.fromRequest(exchange, rsb);
        if (conf.mergeLocal) {
            return queryData.queryMergedOctetsLocal(reqctx, queryMono);
        }
        else {
            return queryData.queryMergedOctets(reqctx, queryMono);
        }
    }


    @PostMapping(path = "/api/1/query", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<Flux<DataBuffer>>> queryProducesJson(ServerWebExchange exchange, @RequestBody Mono<Query> queryMono) {
        totalDataRequests.getAndAdd(1);
        ReqCtx reqctx = ReqCtx.fromRequest(exchange, rsb);
        if (!exchange.getRequest().getHeaders().getAccept().contains(MediaType.APPLICATION_JSON)) {
            LOGGER.warn("{}  /query for unpack without Accept header", reqctx);
        }
        return queryData.queryMergedJson(reqctx, queryMono);
    }


    @PostMapping(path = "/api/1/queryMerged", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public Mono<ResponseEntity<Flux<DataBuffer>>> queryMergedOctets(ServerWebExchange exchange, @RequestBody Mono<Query> queryMono) {
        totalDataRequests.getAndAdd(1);
        return queryData.queryMergedOctets(ReqCtx.fromRequest(exchange, rsb), queryMono);
    }

    @PostMapping(path = "/api/1/queryNoMerge", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public Mono<ResponseEntity<Flux<DataBuffer>>> queryLocal(ServerWebExchange exchange, @RequestBody Mono<Query> queryMono) {
        totalDataRequests.getAndAdd(1);
        return queryData.queryNoMerge(ReqCtx.fromRequest(exchange, rsb), exchange, queryMono);
    }

    @PostMapping(path = "/api/1/rng", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public Mono<ResponseEntity<Flux<DataBuffer>>> rng(ServerWebExchange exchange, @RequestBody Mono<Query> queryMono) {
        final int N = 64 * 1024;
        byte[] load = new byte[N];
        DataBufferFactory bufFac = exchange.getResponse().bufferFactory();
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

    @GetMapping(path = "/api/1/rngB/{seed}/{rate}", produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public ResponseEntity<Flux<ByteBuffer>> rngB(ServerWebExchange exchange, @PathVariable int seed, @PathVariable int rate) {
        final int N = 32;
        int[] state = new int[] { seed, 0 };
        Flux<ByteBuffer> fl = Flux.range(0, 1024 * 1024 * 1024)
        .map(n -> {
            ByteBuffer bb = ByteBuffer.allocate(N);
            for (int i = 0; i < N; i += 1) {
                int h = state[0];
                bb.put((byte) h);
                h ^= h << 13;
                h ^= h >> 17;
                h ^= h << 5;
                state[0] = h;
                state[1] += 1;
            }
            bb.flip();
            return bb;
        })
        .transform(k -> Throttle.throttleByteBuffer(k, rate, 16, 100, 100))
        .doOnNext(k -> QueryData.totalBytesEmitted.getAndAdd(k.remaining()));
        return ResponseEntity.ok().contentType(MediaType.APPLICATION_OCTET_STREAM).body(fl);
    }

    @PostMapping(path = "/api/1/rawLocal", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public Mono<ResponseEntity<Flux<DataBuffer>>> rawLocal(ServerWebExchange exchange, @RequestBody Mono<Query> queryMono) {
        totalDataRequests.getAndAdd(1);
        return queryData.rawLocal(ReqCtx.fromRequest(exchange, false, rsb), exchange, queryMono);
    }

    @PostMapping(path = "/api/1/queryMergedLocal", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public Mono<ResponseEntity<Flux<DataBuffer>>> queryMergedLocalOctets(ServerWebExchange exchange, @RequestBody Mono<Query> queryMono) {
        totalDataRequests.getAndAdd(1);
        return queryData.queryMergedOctetsLocal(ReqCtx.fromRequest(exchange, rsb), queryMono);
    }

    Flux<DataBuffer> channelsJson(DataBufferFactory bufFac, ChannelSearchQuery q, boolean configOut) {
        return Flux.generate(() -> ChannelLister.create(conf, bufFac, q.order(), q.regex, q.sourceRegex, q.descriptionRegex, configOut), ChannelLister::generate, ChannelLister::release)
        .subscribeOn(dbsched)
        .concatMapIterable(Function.identity(), 1);
    }

    @GetMapping(path = "/api/1/channels", produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<Flux<DataBuffer>>> channelsGet(ServerWebExchange exchange) {
        ReqCtx reqctx = ReqCtx.fromRequest(exchange, rsb);
        LOGGER.debug("{}  request for channelsGet", reqctx);
        ChannelSearchQuery q = new ChannelSearchQuery();
        q.ordering = "asc";
        return Mono.just(channelsJson(exchange.getResponse().bufferFactory(), q, false))
        .map(fl -> {
            LOGGER.info("{}  building response entity", reqctx);
            return ResponseEntity.ok()
            .header("X-CanonicalHostname", conf.canonicalHostname)
            .contentType(MediaType.APPLICATION_JSON)
            .body(fl);
        });
    }

    @GetMapping(path = "/api/1/channels/search/regexp/{regexp}", produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<Flux<DataBuffer>>> channelsGet(ServerWebExchange exchange, @PathVariable String regexp) {
        ReqCtx reqctx = ReqCtx.fromRequest(exchange, rsb);
        LOGGER.info("{}  request for channelsRegexp  [{}]", reqctx, regexp);
        ChannelSearchQuery q = new ChannelSearchQuery();
        q.ordering = "asc";
        return Mono.just(channelsJson(exchange.getResponse().bufferFactory(), q, false))
        .map(fl -> {
            return ResponseEntity.ok()
            .header("X-CanonicalHostname", conf.canonicalHostname)
            .contentType(MediaType.APPLICATION_JSON)
            .body(fl);
        });
    }

    @PostMapping(path = "/api/1/channels", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<Flux<DataBuffer>>> channelsPost(ServerWebExchange exchange, @RequestBody Mono<ChannelSearchQuery> queryMono) {
        ReqCtx reqctx = ReqCtx.fromRequest(exchange, rsb);
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
            .header("X-CanonicalHostname", conf.canonicalHostname)
            .contentType(MediaType.APPLICATION_JSON)
            .body(fl);
        });
    }

    @PostMapping(path = "/api/1/channels/config", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public Mono<ResponseEntity<Flux<DataBuffer>>> channelsConfigPost(ServerWebExchange exchange, @RequestBody Mono<ChannelSearchQuery> queryMono) {
        ReqCtx reqctx = ReqCtx.fromRequest(exchange, rsb);
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
            .header("X-CanonicalHostname", conf.canonicalHostname)
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

    public static void logHeaders(ReqCtx reqCtx, ServerWebExchange ex) {
        for (String n : List.of("User-Agent", "X-PythonDataAPIPackageVersion", "X-PythonDataAPIModule")) {
            LOGGER.info("{}  header {} {}", reqCtx, n, ex.getRequest().getHeaders().get(n));
        }
    }

    @GetMapping(path = "/api/1/requestStatus/{reqid}", produces = MediaType.APPLICATION_JSON_VALUE)
    public RequestStatus requestStatus(ServerWebExchange ex, @PathVariable String reqid) {
        return queryData.requestStatusBoard().get(reqid);
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
        return mapper.readValue(f1, ConfigurationRetrieval.class);
    }

    ConfigurationRetrieval loadConfiguration(WebServerInitializedEvent ev) throws IOException {
        if (configFile != null) {
            LOGGER.info("try file: {}", configFile);
            File f1 = ResourceUtils.getFile(configFile);
            LOGGER.info("load from: {}", f1);
            return loadConfiguration(f1);
        }
        else {
            return null;
        }
    }

    @PreDestroy
    public void preDestroy() {
        LOGGER.info("preDestroy");
        shuttingDown.set(1);
    }

    @Override
    public void destroy() {
        LOGGER.info("destroy");
        shuttingDown.set(1);
    }

    @Override
    public void onApplicationEvent(WebServerInitializedEvent ev) {
        LOGGER.warn("test warn level log output");
        LOGGER.error("test error level log output");
        localPort = ev.getWebServer().getPort();
        try {
            ConfigurationRetrieval c = loadConfiguration(ev);
            c.validate();
            conf = c;
        }
        catch (ConfigurationRetrieval.InvalidException e) {
            LOGGER.error("Invalid configuration: {}", e.toString());
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        Hooks.onOperatorDebug();
        Hooks.onNextDropped(obj -> QueryData.doDiscard("hooks_api1_0", obj));
        Hooks.onOperatorError((err, obj) -> {
            QueryData.doDiscard("Hooks.onOperatorError", obj);
            ByteArrayOutputStream ba = new ByteArrayOutputStream();
            PrintStream ps = new PrintStream(ba);
            err.printStackTrace(ps);
            if (obj instanceof BufCont) {
                BufCont bc = (BufCont) obj;
                ps.format("BufCont Marks %s", bc.marksToString());
            }
            ps.close();
            LOGGER.error("Hooks.onOperatorError  {}  {}\n{}", err, obj, ba.toString(StandardCharsets.UTF_8));
            return err;
        });
        LOGGER.info("canonicalHostname {}  localPort {}  databufferBaseDir {}  databufferKeyspacePrefix {}", conf.canonicalHostname, localPort, conf.databufferBaseDir, conf.databufferKeyspacePrefix);
        queryData = new QueryData(new BaseDirFinderFormatV0(Path.of(conf.databufferBaseDir), conf.databufferKeyspacePrefix), conf);
        queryData.port = localPort;
        rsb = queryData.requestStatusBoard();
        new RawSub(queryData).rawTcp();
    }

    public final AtomicInteger shuttingDown = new AtomicInteger();

}
