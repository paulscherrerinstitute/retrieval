package ch.psi.daq.retrieval.controller;

import ch.psi.daq.retrieval.reqctx.ReqCtx;
import ch.psi.daq.retrieval.bytes.BufCont;
import ch.qos.logback.classic.Logger;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@RestController
public class ServiceInfo {
    static final Logger LOGGER = (Logger) LoggerFactory.getLogger(ServiceInfo.class.getSimpleName());
    public static Class<?> profClass;
    final API_1_0_1 api101;

    public ServiceInfo(API_1_0_1 api101) {
        this.api101 = api101;
    }

    @GetMapping(path = "/api/1/stats/version")
    public String version() throws IOException {
        Properties props = new Properties();
        props.load(new DefaultResourceLoader().getResource("info.properties").getInputStream());
        return props.getProperty("appVersion") + "[" + props.getProperty("commitDescribe") + "]" + " (" + props.getProperty("buildDateTime") + ")";
    }

    @GetMapping(path = "/api/1/stats/overview")
    public Overview overview(@RequestParam Map<String, String> params) throws IOException {
        Properties props = new Properties();
        try {
            props.load(new DefaultResourceLoader().getResource("info.properties").getInputStream());
        }
        catch (FileNotFoundException e) {
            props = null;
        }
        Overview ret = new Overview();
        ret.ts = ZonedDateTime.now(ZoneOffset.UTC);
        if (props != null) {
            ret.version = props.getProperty("appVersion");
            ret.buildTime = props.getProperty("buildDateTime");
            ret.commitDescribe = props.getProperty("commitDescribe");
        }
        ret.statusMapCount = api101.queryData.requestStatusBoard.mapCount();
        try {
            Object inforet = profClass.getMethod("describe").invoke(null);
            if (inforet != null) {
                ret.info = new ObjectMapper().readTree(inforet.toString());
            }
        }
        catch (Throwable e) {
        }
        try {
            ret.showTest = profClass.getMethod(params.get("showMethod"), Map.class)
            .invoke(null, params).toString();
        }
        catch (Throwable e) {
        }
        ret.totalDataRequests = api101.totalDataRequests.get();
        ret.configFile = api101.configFile;
        ret.requestStatusBoardStats = api101.queryData.requestStatusBoard().stats();
        return ret;
    }

    @GetMapping(path = "/api/1/stats/buffer/marker/values")
    public List<String> bufferMarkerValues() {
        return BufCont.markValues();
    }

    @GetMapping(path = "/api/1/stats/buffer/marker/hist/counts")
    public List<Long> bufferMarkerHistCounts() {
        return BufCont.markHistCounts();
    }

    @GetMapping(path = "/api/1/stats/buffer/trace")
    public byte[] bufferTrace() {
        return BufCont.listOpen().getBytes(StandardCharsets.UTF_8);
    }

    @GetMapping(path = "/api/1/stats/gc")
    public String doGc() {
        System.gc();
        return "";
    }

    @GetMapping(path = "/api/1/stats/leakbufcont")
    public void leakbufcont(ServerWebExchange exchange) {
        BufCont.allocate(exchange.getResponse().bufferFactory(), 1234, BufCont.Mark.TestA);
    }

    public static class Datafile {
        public FileTime mtime;
        public long flen;
        public Datafile(FileTime mtime, long flen) {
            this.mtime = mtime;
            this.flen = flen;
        }
        public static int compareModTime(Datafile a, Datafile b) {
            return a.mtime.compareTo(b.mtime);
        }
    }

    public static class Split {
        public int sp;
        public List<Datafile> datafiles = new ArrayList<>();
        public Split(int sp) {
            this.sp = sp;
        }
        public long fileCount() {
            return datafiles.size();
        }
    }

    static Split scanSplit(DataRepo dr, Path baseKsData, String channel, int tb, int sp, SearchParams searchParams) throws IOException {
        Split ret = new Split(sp);
        Path dir = baseKsData.resolve(channel).resolve(String.format("%019d", tb)).resolve(String.format("%010d", sp));
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            for (Path entry : stream) {
                if (entry.getFileName().toString().endsWith("_Data")) {
                    FileTime mtime = Files.getLastModifiedTime(entry);
                    long flen = Files.size(entry);
                    if (mtime.toInstant().isAfter(searchParams.minMtime) && flen >= searchParams.minFileLen) {
                        ret.datafiles.add(new Datafile(mtime, flen));
                        searchParams.foundFiles += 1;
                    }
                }
                if (searchParams.stop()) {
                    break;
                }
            }
        }
        return ret;
    }

    public static class Timebin {
        public int tb;
        public List<Split> splits = new ArrayList<>();
        public Timebin(int tb) {
            this.tb = tb;
        }
        public long fileCount() {
            return splits.stream().map(Split::fileCount).reduce(0L, Long::sum);
        }
    }

    static Timebin scanTimebin(DataRepo dr, Path baseKsData, String channel, int tb, SearchParams searchParams) throws IOException {
        Timebin ret = new Timebin(tb);
        Path dir = baseKsData.resolve(channel).resolve(String.format("%019d", tb));
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            for (Path entry : stream) {
                try {
                    int sp = Integer.parseInt(entry.getFileName().toString(), 10);
                    ret.splits.add(scanSplit(dr, baseKsData, channel, tb, sp, searchParams));
                }
                catch (NumberFormatException e) {
                }
                if (searchParams.stop()) {
                    break;
                }
            }
        }
        return ret;
    }

    public static class Channel {
        public String name;
        public int ks;
        public List<Timebin> timebins = new ArrayList<>();
        public Channel(String name, int ks) {
            this.name = name;
            this.ks = ks;
        }
        public long fileCount() {
            return timebins.stream().map(Timebin::fileCount).reduce(0L, Long::sum);
        }
    }

    public static Channel scanChannel(DataRepo dr, Path baseKsData, int ks, String channel, SearchParams searchParams) throws IOException {
        Channel ret = new Channel(channel, ks);
        Path dir = baseKsData.resolve(channel);
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            for (Path entry : stream) {
                try {
                    int tb = Integer.parseInt(entry.getFileName().toString(), 10);
                    ret.timebins.add(scanTimebin(dr, baseKsData, channel, tb, searchParams));
                }
                catch (NumberFormatException e) {
                }
                if (searchParams.stop()) {
                    break;
                }
            }
        }
        return ret;
    }

    static class Keyspace {
        int ks;
        List<Channel> channels = new ArrayList<>();
        Keyspace(int ks) {
            this.ks = ks;
        }
        long fileCount() {
            return channels.stream().map(Channel::fileCount).reduce(0L, Long::sum);
        }
    }

    Keyspace scanKeyspace(DataRepo dr, int ks, SearchParams searchParams) throws IOException {
        Keyspace ret = new Keyspace(ks);
        Path dir = Path.of(api101.conf.databufferBaseDir).resolve(String.format("%s_%d", api101.conf.databufferKeyspacePrefix, ks)).resolve("byTime");
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            for (Path entry : stream) {
                ret.channels.add(scanChannel(dr, dir, ks, entry.getFileName().toString(), searchParams));
                if (searchParams.stop()) {
                    break;
                }
            }
        }
        catch (NoSuchFileException e) {
        }
        return ret;
    }

    public static class DataRepo {
        public List<Keyspace> keyspaces = new ArrayList<>();
        public long fileCount() {
            return keyspaces.stream().map(Keyspace::fileCount).reduce(0L, Long::sum);
        }
    }

    public static class ChannelInfo {
        public int ks;
        public String name;
        public long dataBytes;
        ChannelInfo(int ks, String name, long dataBytes) {
            this.ks = ks;
            this.name = name;
            this.dataBytes = dataBytes;
        }
    }

    public static class DataActive {
        public List<ChannelInfo> channels = new ArrayList<>();
        void sort() {
            channels = channels.stream().sorted((a, b) -> 0).collect(Collectors.toList());
        }
        DataActive(DataRepo dr) {
            dr.keyspaces.stream()
            .forEach(ks -> {
                ks.channels.stream().forEach(ch -> {
                    long dataBytes = ch.timebins.stream()
                    .flatMap(tb -> tb.splits.stream())
                    .flatMap(sp -> sp.datafiles.stream())
                    //.sorted(Datafile::compareModTime)
                    //.max(Datafile::compareModTime)
                    .map(df -> df.flen)
                    .reduce(0L, (a, x) -> a + x);
                    channels.add(new ChannelInfo(ks.ks, ch.name, dataBytes));
                });
            });
        }
    }

    public static class SearchParams {
        public long minFileLen;
        public Instant minMtime;
        public Duration maxSec;
        public int maxFiles;
        public int foundFiles;
        public Instant tsBeg;
        public int ks;
        public SearchParams() {
            tsBeg = Instant.now();
        }
        public boolean stop() {
            return foundFiles >= maxFiles || tsBeg.isBefore(Instant.now().minus(maxSec));
        }
    }

    @GetMapping(path = "/api/1/stats/data/active/{maxFiles}/{maxSec}/{maxAge}/{minFileLenMb}")
    public DataActive dataActive(@RequestParam Map<String, String> params, @PathVariable("maxFiles") int maxFiles, @PathVariable("maxSec") int maxSec, @PathVariable("maxAge") int maxAge, @PathVariable("minFileLenMb") int minFileLenMb) throws IOException {
        SearchParams searchParams = new SearchParams();
        searchParams.minFileLen = (long) minFileLenMb * 1024 * 1024;
        searchParams.maxFiles = maxFiles;
        searchParams.maxSec = Duration.ofSeconds(maxSec);
        searchParams.minMtime = Instant.now().minus(Duration.ofMinutes(maxAge));
        if (params.containsKey("ks")) {
            searchParams.ks = Integer.parseInt(params.get("ks"), 10);
        }
        //LOGGER.info("maxFiles {}", searchParams.maxFiles);
        //LOGGER.info("maxSec {}", searchParams.maxSec);
        //LOGGER.info("minMtime {}", searchParams.minMtime);
        DataRepo dr = new DataRepo();
        for (int ks : List.of(2, 3, 4)) {
            if (searchParams.ks == 0 || searchParams.ks == ks) {
                dr.keyspaces.add(scanKeyspace(dr, ks, searchParams));
                if (searchParams.stop()) {
                    break;
                }
            }
        }
        return new DataActive(dr);
    }

    @GetMapping(path = "/leak/{nn0}/{nn1}")
    public Mono<String> leak(ServerWebExchange exchange, @PathVariable int nn0, @PathVariable int nn1) {
        DataBufferFactory bufFac = exchange.getResponse().bufferFactory();
        List<DataBuffer> l1 = new ArrayList<>();
        /*
        Stream.iterate(0, x -> x != 2000, x -> x + 1)
        .forEach(k -> {
            l1.add(bufFac.allocateBuffer(8 * 1024));
        });
        */
        for (int i1 = 0; i1 < nn0; i1 += 1) {
            l1.add(bufFac.allocateBuffer(8 * 1024));
        }
        AtomicLong totalBytes = new AtomicLong();
        return Flux.range(0, nn1)
        .flatMap(i1 -> {
            return WebClient.create()
            .get().uri("http://localhost:8331/api/1/stats/version")
            .exchangeToMono(res -> {
                if (res.statusCode() == HttpStatus.OK) {
                    return res.bodyToFlux(DataBuffer.class)
                    .map(DataBuffer::readableByteCount)
                    .reduce(0L, Long::sum);
                }
                else {
                    return Mono.error(new RuntimeException("fail"));
                }
            });
        })
        .reduce(0L, Long::sum)
        .map(k -> String.format("leaked: %d  %d  %d\n", l1.size(), totalBytes.get(), k));
    }

    @GetMapping(path = "/logtest")
    public String version(ServerWebExchange exchange) {
        ReqCtx reqctx = ReqCtx.fromRequest(exchange, api101.rsb);
        LOGGER.info("plain info");
        LOGGER.debug("plain debug");
        LOGGER.trace("plain trace");
        LOGGER.info("{} info with reqid", reqctx);
        LOGGER.debug("{} debug with reqid", reqctx);
        LOGGER.trace("{} trace with reqid", reqctx);
        return "ok";
    }

    @GetMapping(path = "/static/opt/ret/{pathString}")
    public ResponseEntity<DataBuffer> staticOptRet(@PathVariable String pathString) throws IOException {
        InputStream inp = null;
        String html = null;
        //html = StandardCharsets.UTF_8.decode(ByteBuffer.wrap(inp.readAllBytes())).toString();
        DataBufferFactory bufFac = new DefaultDataBufferFactory();
        html = String.format("%s", pathString);
        if (pathString.matches(".*(//|\\.\\.).*")) {
            ByteBuffer buf1 = StandardCharsets.UTF_8.encode("invalid path: " + pathString);
            DataBuffer buf2 = new DefaultDataBufferFactory().wrap(buf1);
            return ResponseEntity.badRequest().body(buf2);
        }
        Path path = Path.of("/opt/retrieval").resolve(pathString);
        if (!Files.isRegularFile(path)) {
            ByteBuffer buf1 = StandardCharsets.UTF_8.encode("not a file: " + pathString);
            DataBuffer buf2 = bufFac.wrap(buf1);
            return ResponseEntity.badRequest().body(buf2);
        }
        return ResponseEntity.ok(bufFac.wrap(Files.readAllBytes(path)));
    }

    @GetMapping(path = "/catch/**")
    public String catchAll(ServerHttpRequest req) {
        return String.format("id %s  path [%s]  URI path [%s]  URI query [%s]", req.getId(), req.getPath(), req.getURI().getPath(), req.getURI().getQuery());
    }

}
