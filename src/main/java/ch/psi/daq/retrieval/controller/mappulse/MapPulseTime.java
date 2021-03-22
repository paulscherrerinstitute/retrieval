package ch.psi.daq.retrieval.controller.mappulse;

import ch.psi.daq.retrieval.reqctx.ReqCtx;
import ch.psi.daq.retrieval.config.ConfigurationDatabase;
import ch.psi.daq.retrieval.controller.API_1_0_1;
import ch.psi.daq.retrieval.controller.ServiceInfo;
import ch.psi.daq.retrieval.error.RetrievalException;
import ch.qos.logback.classic.Logger;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.postgresql.util.PSQLException;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RestController
public class MapPulseTime {
    static final Logger LOGGER = (Logger) LoggerFactory.getLogger(MapPulseTime.class.getSimpleName());

    @GetMapping(path = "/api/1/map/pulse/local/{pulse}")
    public Mono<Deque<Long>> mapPulseTsLocal(ServerWebExchange exchange, @PathVariable long pulse) {
        return mapPulse(pulse)
        .map(k -> k.ts)
        .doOnError(e -> {
            LOGGER.error("ERROR  {}", e.toString());
            throw new RuntimeException(e);
        })
        .subscribeOn(Schedulers.boundedElastic())
        //.doOnNext(k -> LOGGER.info("mapPulseTsLocal  pulse {}  list {}", pulse, new ArrayList<>(k)))
        .map(k -> {
            if (pulse == 43) {
                throw new RuntimeException("test-error in mapPulseTsLocal");
            }
            else {
                return k;
            }
        });
    }

    @GetMapping(path = "/api/1/map/pulse/locallog/{pulse}")
    public Mono<StringBuilder> mapPulseLocalLog(ServerWebExchange exchange, @PathVariable long pulse) {
        ReqCtx reqCtx = ReqCtx.fromRequest(exchange, api1.rsb);
        return mapPulse(pulse)
        .map(k -> k.sb)
        .doOnError(e -> {
            LOGGER.error("ERROR  {}", e.toString());
            throw new RuntimeException(e);
        })
        .subscribeOn(Schedulers.boundedElastic());
    }

    static final DateTimeFormatter datefmt = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSz");

    @GetMapping(path = "/api/1/map/pulse/formatted/{pulse}")
    public Mono<String> mapPulseTsFormatted(@PathVariable long pulse) {
        return mapPulseTsHisto(pulse)
        .map(k -> {
            if (k.size() <= 0) {
                return "";
            }
            else {
                return ZonedDateTime.ofInstant(Instant.ofEpochMilli(k.get(0).get(0) / 1000 / 1000), ZoneId.systemDefault()).format(datefmt);
            }
        });
    }

    @GetMapping(path = "/api/1/map/pulse/{pulse}")
    public Mono<ResponseEntity<Long>> mapPulseNum(@PathVariable long pulse) {
        return mapPulseTsHisto(pulse)
        //.doOnNext(k -> LOGGER.info("mapPulseTsHisto  pulse {}  returns {}", pulse, k))
        .<ResponseEntity<Long>>map(k -> {
            if (k.size() <= 0) {
                return ResponseEntity.notFound().build();
            }
            else if (k.stream().anyMatch(q -> q.size() != 2) || pulse == 42) {
                return ResponseEntity.badRequest().build();
            }
            else {
                return ResponseEntity.ok().body(k.get(0).get(0));
            }
        })
        .subscribeOn(Schedulers.boundedElastic());
    }

    static class S {
        long a;
        long b;
    }

    @GetMapping(path = "/api/1/map/pulse/histo/{pulse}")
    public Mono<List<List<Long>>> mapPulseTsHisto(@PathVariable long pulse) {
        return mapPulseTsList(pulse)
        //.doOnNext(k -> LOGGER.info("mapPulseTsList  pulse {}  returns {}", pulse, k))
        .flatMapIterable(k -> k)
        .reduce(new TreeMap<Long, Long>(), (a, x) -> {
            if (a.containsKey(x)) {
                a.put(x, a.get(x) + 1);
            }
            else {
                a.put(x, 1L);
            }
            return a;
        })
        .map(k -> {
            List<LongTup> all = new ArrayList<>();
            k.forEach((a, b) -> all.add(new LongTup(a, b)));
            all.sort((a, b) -> -Long.compare(a.b, b.b));
            return all;
        })
        .map(k -> {
            return k.stream().map(q -> List.of(q.a, q.b)).collect(Collectors.toList());
        })
        .subscribeOn(Schedulers.boundedElastic());
    }

    static class LongTup {
        LongTup(long a, long b) {
            this.a = a;
            this.b = b;
        }
        long a;
        long b;
    }

    @GetMapping(path = "/api/1/map/pulse/list/{pulse}")
    public Mono<ArrayDeque<Long>> mapPulseTsList(@PathVariable long pulse) {
        return Flux.fromIterable(api1.conf.nodes)
        .flatMap(node -> {
            return WebClient.create().get()
            .uri(String.format("http://%s:%d/api/1/map/pulse/local/%d", node.host, node.port, pulse))
            .retrieve().toEntity(JsonNode.class)
            .doOnError(e -> LOGGER.error("mapPulseTsList  error from subreq {}", e.toString()))
            .map(res -> {
                if (res.hasBody()) {
                    return res.getBody();
                }
                else {
                    return JsonNodeFactory.instance.arrayNode();
                }
            })
            .map(k -> {
                Deque<Long> a = new ArrayDeque<>();
                if (k.isArray()) {
                    for (int i = 0; i < k.size(); i += 1) {
                        a.add(k.get(i).asLong());
                    }
                }
                return a;
            });
        }, 20, 20)
        .reduce(new ArrayDeque<Long>(), (a, x) -> {
            a.addAll(x);
            return a;
        })
        .subscribeOn(Schedulers.boundedElastic());
    }

    static class MappedPulse extends LogRes<MappedPulse> {
        Deque<Long> ts = new ArrayDeque<>();
    }

    Mono<MappedPulse> mapPulse(long pulse) {
        MappedPulse ret = new MappedPulse();
        ret.println("mapPulse  %d", pulse);
        return connection()
        .flatMap(conn -> {
            ret.println("calling listFileCandidates  %d", pulse);
            return listFileCandidates(conn, pulse)
            .flatMapMany(k -> {
                ret.println("mapPulse: appending to...");
                ret.append(k);
                return Flux.fromIterable(k.dataPaths)
                .flatMap(q -> {
                    return searchPulseTime(pulse, q)
                    .onErrorResume(e -> {
                        LOGGER.error("searchPulseTime  pulse {}  path {}  error {}", pulse, q.path.toString(), e.toString());
                        return Mono.empty();
                    })
                    ;
                }, 4)
                .filter(q -> q.ts >= 0)
                .map(q -> {
                    ret.println("hit   %d   in %s", q.ts, q.dataPath.path);
                    return q.ts;
                });
            })
            //.doOnNext(q -> LOGGER.info("pulse {}  long {}", pulse, q))
            .collect(Collectors.toList())
            .doOnNext(k -> {
                ret.ts.addAll(k);
            })
            .doFinally(k -> closeConn(conn));
        })
        .then(Mono.just(ret))
        .doOnNext(k -> {
            k.ts.forEach(ts -> {
                ret.println("FOUND ts %d", ts);
            });
        });
    }

    static class FileCandidates extends LogRes<FileCandidates> {
        Deque<DataPath> dataPaths = new ArrayDeque<>();
    }

    Mono<FileCandidates> listFileCandidates(Connection conn, long pulse) {
        return Mono.defer(() -> {
            FileCandidates ret = new FileCandidates();
            ret.println("listFileCandidates  %d", pulse);
            try (PreparedStatement st = conn.prepareStatement("select channel, split, timebin from map_pulse_files where split in ("+localSplitsString()+") and pulse_min <= ? and (closed = 0 or pulse_max >= ?)")) {
                st.setLong(1, pulse);
                st.setLong(2, pulse);
                try (ResultSet res = st.executeQuery()) {
                    while (res.next()) {
                        ret.println("cand  %s  %d  %d", res.getString(1), res.getInt(2), res.getLong(3));
                        DataPath dp = new DataPath();
                        dp.channel = res.getString(1);
                        dp.split = res.getInt(2);
                        dp.tb = res.getLong(3);
                        dp.path = datafilePath(dp);
                        ret.dataPaths.add(dp);
                    }
                }
                return Mono.just(ret);
            }
            catch (SQLException e) {
                return Mono.error(e);
            }
            catch (Throwable e) {
                LOGGER.error("in listFileCandidates  {}", e.toString());
                return Mono.error(e);
            }
        });
    }

    @GetMapping(path = "/api/1/map/index")
    public Mono<StringBuilder> mapIndex() {
        return connection()
        .flatMap(conn -> {
            return Mono.empty()
            //.then(dropTables(conn))
            .then(createTables(conn))
            //.then(scanFiles(reqCtx, conn))
            .then(scanTimers())
            .doFinally(sig -> {
                closeConn(conn);
            });
        })
        .map(k -> k.sb)
        .subscribeOn(Schedulers.boundedElastic());
    }

    Mono<Connection> connection() {
        ConfigurationDatabase c = api1.conf.database;
        try {
            return Mono.just(DriverManager.getConnection(String.format("jdbc:postgresql://%s:%d/%s", c.host, c.port, c.database), c.username, c.password));
        }
        catch (SQLException e) {
            LOGGER.error("can no connect {} {} {} {} {}", c.host, c.port, c.database, c.username, e.toString());
            return Mono.error(new RetrievalException("request error"));
        }
    }

    Mono<Void> dropTables(Connection conn) {
        try {
            {
                PreparedStatement st = conn.prepareStatement("drop table map_pulse_channels");
                //st.setString(1, "abc");
                st.execute();
                st.close();
            }
            {
                PreparedStatement st = conn.prepareStatement("drop table map_pulse_files");
                st.execute();
                st.close();
            }
            return Mono.empty();
        }
        catch (SQLException e) {
            if (e.getMessage().contains("does not exist")) {
                return Mono.empty();
            }
            else {
                return Mono.error(e);
            }
        }
    }

    Mono<Void> createTables(Connection conn) {
        sqlExec(conn, "create table if not exists map_pulse_channels (name text, tbmax int)");
        sqlExec(conn, "create table if not exists map_pulse_files (channel text not null, split int not null, timebin int not null, closed int not null default 0, pulse_min int8 not null, pulse_max int8 not null)");
        sqlExec(conn, "create unique index if not exists map_pulse_files_ix1 on map_pulse_files (channel, split, timebin)");
        return Mono.empty();
    }

    Mono<Void> sqlExec(Connection conn, String sql) {
        try (PreparedStatement st = conn.prepareStatement(sql)) {
            st.execute();
            return Mono.empty();
        }
        catch (SQLException e) {
            return Mono.error(e);
        }
    }

    void closeConn(Connection conn) {
        try {
            conn.close();
        }
        catch (SQLException e) {
        }
    }

    Flux<String> fileSummary(ReqCtx reqCtx, String channelName) {
        Instant beg = Instant.ofEpochSecond(10000);
        Instant tsBeg = beg;
        Instant tsEnd = Instant.ofEpochSecond(2100000000);
        List<Integer> splits = List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14);
        return Flux.fromIterable(splits)
        .concatMap(split -> {
            return api1.queryData.baseDirFinder.findMatchingDataFiles(reqCtx, channelName, tsBeg, tsEnd, split)
            .flatMapMany(res -> {
                return res.timeBins
                .map(tb -> {
                    return String.format("%s  ks %s  sp %d  bs %d  tb %d", res.locatedChannel.name, res.ks, res.sp, tb.binSize, tb.timeBin);
                });
            });
        }, 0);
    }

    Mono<StringBuilder> scanFiles(ReqCtx reqCtx, Connection conn) {
        return Flux.just("SAR-CVME-TIFALL5:EvtSet")
        .concatMap(channelName -> {
            return Flux.just(3)
            .concatMap(ks -> {
                return scanChannel(reqCtx, ks, channelName)
                .flatMapMany(ch -> {
                    //LOGGER.info("AAAAAAAAA  {}", channelName);
                    Stream<String> st = ch.timebins.stream()
                    //.peek(k -> LOGGER.info("DD {}", k))
                    .flatMap(tb -> tb.splits.stream())
                    //.peek(k -> LOGGER.info("EE {}", k))
                    .flatMap(sp -> sp.datafiles.stream())
                    .map(df -> {
                        return String.format("scanFiles  len %d", df.flen);
                    });
                    return Flux.fromStream(st);
                });
            });
        })
        .reduce(new StringBuilder(), (a, x) -> a.append(x).append('\n'));
    }

    Mono<ServiceInfo.Channel> scanChannel(ReqCtx reqCtx, int ks, String channelName) {
        return Mono.fromCallable(() -> {
            Path dir = Path.of(api1.conf.databufferBaseDir)
            .resolve(String.format("%s_%d", api1.conf.databufferKeyspacePrefix, ks))
            .resolve("byTime");
            ServiceInfo.DataRepo repo = new ServiceInfo.DataRepo();
            ServiceInfo.SearchParams sp = new ServiceInfo.SearchParams();
            sp.minFileLen = 0;
            sp.minMtime = Instant.EPOCH;
            sp.maxFiles = Integer.MAX_VALUE;
            sp.maxSec = Duration.ofSeconds(30);
            return ServiceInfo.scanChannel(repo, dir, ks, channelName, sp);
        }).subscribeOn(Schedulers.boundedElastic());
    }

    Flux<Path> pathsForChannel(ReqCtx reqCtx, String channelName) {
        Instant tsBeg = Instant.ofEpochSecond(10000);
        Instant tsEnd = Instant.ofEpochSecond(2100000000);
        List<Integer> splits = List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14);
        return Flux.fromIterable(splits)
        .concatMap(split -> {
            return api1.queryData.baseDirFinder.findMatchingDataFiles(reqCtx, channelName, tsBeg, tsEnd, split)
            .flatMapMany(res -> {
                return res.timeBins
                .map(tb -> {
                    return res.dataPath(tb);
                });
            });
        }, 0);
    }

    List<String> timerChannelNames() {
        if (api1.conf.testPulseMap) {
            return List.of("test-scalar-01");
        }
        else {
            /*
            Examples:
            SINEG01-RLLE-STA:MASTER-EVRPULSEID
            SINSB01-RLLE-STA:MASTER-EVRPULSEID
            */
            List<String> sections = List.of("SINEG01", "SINSB01", "SINSB02", "SINSB03", "SINSB04", "SINXB01");
            List<String> suffix = List.of("MASTER");
            return sections.stream()
            .flatMap(sec -> suffix.stream().map(suf -> sec + "-RLLE-STA:" + suf + "-EVRPULSEID"))
            //.limit(12)
            .collect(Collectors.toList());
        }
    }

    static class TimerScan extends LogRes<TimerScan> {
    }

    Mono<TimerScan> scanTimers() {
        Connection conn = connection().block();
        TimerScan ret = new TimerScan();
        ret.println("TEST OUTPUT");
        return Flux.fromIterable(timerChannelNames())
        .doOnNext(k -> ret.println("scanTimers channel %s", k))
        .flatMap(k -> scanChannel(k).onErrorResume(e -> {
            LOGGER.error("{}", e.toString());
            ret.println(e.toString());
            return Mono.empty();
        }), 4)
        .doOnNext(k -> {
            ret.append(k);
            k.bounds.forEach(q -> {
                ret.println("bounds  %d  %d  %d  %d  %s", q.pulseFirst, q.pulseLast, q.dataPath.tb, q.dataPath.split, q.dataPath.channel);
            });
        })
        .concatMap(k -> updateDb(k, conn))
        .onErrorResume(e -> {
            if (e instanceof PSQLException) {
                if (api1.shuttingDown.get() == 0) {
                    return Mono.error(e);
                }
                else {
                    LOGGER.info("Database error while shutdown (that's expected)");
                    return Mono.empty();
                }
            }
            else {
                return Mono.error(e);
            }
        })
        .doFinally(k -> closeConn(conn))
        .then(Mono.just(ret));
    }

    Mono<Void> updateDb(ChannelScan channelScan, Connection conn) {
        return Mono.defer(() -> {
            for (DatafileBounds bounds : channelScan.bounds) {
                try (PreparedStatement st = conn.prepareStatement("insert into map_pulse_files (channel, split, timebin, pulse_min, pulse_max) values (?, ?, ?, ?, ?) on conflict (channel, split, timebin) do update set pulse_min=?, pulse_max=?")) {
                    st.setString(1, bounds.dataPath.channel);
                    st.setInt(2, bounds.dataPath.split);
                    st.setInt(3, (int) bounds.dataPath.tb);
                    st.setLong(4, bounds.pulseFirst);
                    st.setLong(5, bounds.pulseLast);
                    st.setLong(6, bounds.pulseFirst);
                    st.setLong(7, bounds.pulseLast);
                    st.execute();
                }
                catch (SQLException e) {
                    return Mono.error(new RuntimeException(e));
                }
            }
            return Mono.empty();
        });
    }

    static class ChannelScan extends LogRes<ChannelScan> {
        String name;
        Deque<DatafileBounds> bounds = new ArrayDeque<>();
    }

    Mono<ChannelScan> scanChannel(String channel) {
        ChannelScan ret = new ChannelScan();
        api1.conf.nodes.forEach(k -> {
            ret.println("node host %s  %s  (%s)", k.host, k.splits, api1.conf.canonicalHostname);
        });
        List<Integer> localSp = localSplits();
        ret.println("scanChannel %s  %s", channel, localSp);
        ret.name = channel;
        return Flux.fromIterable(localSp)
        .concatMap(sp -> dataPathsForChannelSplit(channel, sp), 0)
        .doOnNext(ret::append)
        .concatMap(k -> previewFiles(k), 0)
        .doOnNext(ret::append)
        .doOnNext(k -> {
            ret.bounds.addAll(k.bounds);
        })
        .then(Mono.just(ret));
    }

    static class DataPath {
        String channel;
        int split;
        long tb;
        Path path;
    }

    static class DataPathList extends LogRes<DataPathList> {
        Deque<DataPath> paths = new ArrayDeque<>();
    }

    Path datafilePath(DataPath dp) {
        return Path.of(api1.conf.databufferBaseDir).resolve(api1.conf.databufferKeyspacePrefix + "_2")
        .resolve("byTime").resolve(dp.channel)
        .resolve(String.format("%019d", dp.tb))
        .resolve(String.format("%010d", dp.split))
        .resolve("0000000000086400000_00000_Data");
    }

    Mono<DataPathList> dataPathsForChannelSplit(String channel, int split) {
        return Mono.defer(() -> {
            DataPathList ret = new DataPathList();
            ret.println("scanChannelSplit  %s  %d  thread %s", channel, split, Thread.currentThread().getName());
            Path ksBase = Path.of(api1.conf.databufferBaseDir).resolve(api1.conf.databufferKeyspacePrefix + "_2");
            Path channelDataPath = ksBase.resolve("byTime").resolve(channel);
            Pattern pattern = Pattern.compile("^([0-9]{19})$");
            ret.println("scanChannelSplit  %s  %s  %s  %s", channel, split, ksBase, channelDataPath);
            try (DirectoryStream<Path> ds = Files.newDirectoryStream(channelDataPath)) {
                ds.forEach(p -> {
                    Matcher m = pattern.matcher(p.getFileName().toString());
                    if (m.matches()) {
                        ret.println("matches as timebin   %s    %s", p, m.group(1));
                        DataPath dp = new DataPath();
                        dp.channel = channel;
                        dp.split = split;
                        dp.tb = Long.parseLong(m.group(1));
                        dp.path = p.resolve(String.format("%010d", split)).resolve("0000000000086400000_00000_Data");
                        ret.paths.add(dp);
                    }
                    else {
                        ret.println("DID NOT MATCH   %s", p);
                    }
                });
                ds.close();
                return Mono.just(ret);
            }
            catch (Throwable e) {
                ret.println("scanDataFile  %s  %s  %d", e.toString(), channel, split);
                return Mono.error(ret.toException("scanDataFile " + e.toString()));
            }
        });
    }

    static class PreviewedFiles extends LogRes<PreviewedFiles> {
        Deque<DatafileBounds> bounds = new ArrayDeque<>();
    }

    Mono<PreviewedFiles> previewFiles(DataPathList paths) {
        PreviewedFiles ret = new PreviewedFiles();
        ret.println("previewFiles  paths size %d  thread %s", paths.paths.size(), Thread.currentThread().getName());
        return Mono.just(paths).flatMap(k -> {
            return Flux.fromIterable(k.paths)
            .concatMap(this::scanDataFile)
            .doOnNext(q -> {
                ret.append(q);
                ret.println("previewFiles  %d  %d  %d  %d  %s", q.tsFirst, q.tsLast, q.pulseFirst, q.pulseLast, q.dataPath.path);
                ret.bounds.add(q);
            })
            .then();
        })
        .then(Mono.just(ret));
    }

    static class DatafileBounds extends LogRes<DatafileBounds> {
        DatafileBounds() {}
        DataPath dataPath;
        long firstPos;
        long chunkLen;
        long tsFirst;
        long tsLast;
        long pulseFirst;
        long pulseLast;
    }

    Mono<DatafileBounds> scanDataFile(DataPath dataPath) {
        return Mono.defer(() -> {
            DatafileBounds ret = new DatafileBounds();
            ret.dataPath = dataPath;
            ret.println("scanDataFile  %s  thread %s", dataPath.path, Thread.currentThread().getName());
            try {
                //DataBufferUtils.read(dataPath, DefaultDataBufferFactory.sharedInstance, 1024 * 4);
                Chan chan = Chan.of(Files.newByteChannel(dataPath.path));
                return findFirstChunk(chan)
                .flatMap(k -> {
                    ret.append(k);
                    ret.firstPos = k.pos;
                    return findChunkLength(k.chan, k.pos);
                })
                .flatMap(k -> {
                    ret.append(k);
                    ret.chunkLen = k.len;
                    ret.tsFirst = k.ts;
                    ret.pulseFirst = k.pulse;
                    return readLastChunk(k.chan, ret.firstPos, ret.chunkLen);
                })
                .flatMap(k -> {
                    try {
                        k.chan.take().close();
                    }
                    catch (IOException e) {
                        return Mono.error(e);
                    }
                    if (k.ts < ret.tsFirst) {
                        return Mono.error(ret.toException("tsLast < tsFirst"));
                    }
                    else if (k.pulse < ret.pulseFirst) {
                        return Mono.error(ret.toException("pulseLast < pulseFirst"));
                    }
                    else {
                        ret.tsLast = k.ts;
                        ret.pulseLast = k.pulse;
                        return Mono.just(ret);
                    }
                });
            }
            catch (Throwable e) {
                ret.println("scanDataFile  %s  %s", e.toString(), dataPath);
                return Mono.error(ret.toException(e.toString()));
            }
        });
    }

    static int tryRead(SeekableByteChannel chn, ByteBuffer buf) {
        while (buf.remaining() > 0) {
            try {
                int n1 = chn.read(buf);
                if (n1 <= 0) {
                    break;
                }
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        buf.flip();
        return 0;
    }

    static class FirstChunk extends LogRes<FirstChunk> {
        FirstChunk(Chan chan) {
            this.chan = chan;
        }
        long pos;
        Chan chan;
    }

    Mono<FirstChunk> findFirstChunk(Chan chan) {
        return Mono.defer(() -> {
            SeekableByteChannel chn = chan.take();
            FirstChunk ret = new FirstChunk(Chan.of(chn));
            try {
                ret.println("findFirstChunk  thread %s", Thread.currentThread().getName());
                ByteBuffer buf = ByteBuffer.allocate(1024);
                tryRead(chn, buf);
                if (buf.remaining() < 6) {
                    ret.println("can not even read len  %s", chan);
                    return Mono.just(ret);
                }
                else {
                    buf.getShort();
                    int len1 = buf.getInt();
                    if (len1 <= 8 || len1 > 256) {
                        ret.println("weird len1 %d", len1);
                        return Mono.just(ret);
                    }
                    else {
                        ret.pos = 2L + len1;
                        return Mono.just(ret);
                    }
                }
            }
            catch (Throwable e) {
                ret.println("%s", e.toString());
                return Mono.error(ret.toException("findFirstChunk " + e.toString()));
            }
        });
    }

    static class ChunkLength extends LogRes<ChunkLength> {
        ChunkLength(Chan chan) {
            this.chan = chan;
        }
        long len;
        long ts;
        long pulse;
        Chan chan;
    }

    Mono<ChunkLength> findChunkLength(Chan chan, long first) {
        return Mono.defer(() -> {
            SeekableByteChannel chn = chan.take();
            ChunkLength ret = new ChunkLength(Chan.of(chn));
            try {
                ret.println("findChunkLength  thread %s", Thread.currentThread().getName());
                ByteBuffer buf = ByteBuffer.allocate(1024);
                chn.position(first);
                tryRead(chn, buf);
                if (buf.remaining() < 4 + 8 * 3) {
                    ret.println("can not even read one chunk header");
                    return Mono.just(ret);
                }
                else {
                    int len1 = buf.getInt();
                    long ttl = buf.getLong();
                    long ts = buf.getLong();
                    long pulse = buf.getLong();
                    if (len1 <= 30 || len1 > 128) {
                        ret.println("weird len1 %d", len1);
                        return Mono.just(ret);
                    }
                    else {
                        ret.println("chunk  len %5d  ts %20d  pulse %20d", len1, ts, pulse);
                        ret.len = len1;
                        ret.ts = ts;
                        ret.pulse = pulse;
                        return Mono.just(ret);
                    }
                }
            }
            catch (Throwable e) {
                ret.println(e.toString());
                return Mono.error(ret.toException("findChunkLength " + e.toString()));
            }
        });
    }

    static class LastChunk extends LogRes<LastChunk> {
        LastChunk(Chan chan) {
            this.chan = chan;
        }
        long pos;
        long ts;
        long pulse;
        Chan chan;
    }

    Mono<LastChunk> readLastChunk(Chan chan, long firstPos, long chunkLen) {
        if (chunkLen < 1) {
            return Mono.error(new RuntimeException(String.format("readLastChunk  chunkLen %d", chunkLen)));
        }
        return Mono.defer(() -> {
            SeekableByteChannel chn = chan.take();
            LastChunk ret = new LastChunk(Chan.of(chn));
            try {
                ret.println("readLastChunk  thread %s", Thread.currentThread().getName());
                long flen = chn.size();
                if (flen - firstPos < chunkLen) {
                    ret.println("zero chunks in this file");
                    return Mono.just(ret);
                }
                long p1 = firstPos + ((flen - firstPos) / chunkLen - 1) * chunkLen;
                if (p1 < firstPos) {
                    ret.println("no chunks in this file, why passed?");
                    return Mono.just(ret);
                }
                ByteBuffer buf = ByteBuffer.allocate(1024);
                chn.position(p1);
                tryRead(chn, buf);
                if (buf.remaining() < 4 + 8 * 3) {
                    ret.println("can not even read one chunk header");
                    return Mono.just(ret);
                }
                else {
                    int len1 = buf.getInt();
                    long ttl = buf.getLong();
                    long ts = buf.getLong();
                    long pulse = buf.getLong();
                    if (len1 != chunkLen) {
                        ret.println("chunk len1 mismatch  %d  %d", len1, chunkLen);
                        return Mono.just(ret);
                    }
                    else {
                        ret.pos = p1;
                        ret.ts = ts;
                        ret.pulse = pulse;
                        return Mono.just(ret);
                    }
                }
            }
            catch (Throwable e) {
                ret.println("%s", e.toString());
                return Mono.error(ret.toException("readLastChunk " + e.toString()));
            }
        });
    }

    static class PulseTime extends LogRes<PulseTime> {
        DataPath dataPath;
        long firstPos;
        long chunkLen;
        int reads;
        long ts = -1;
    }

    Mono<PulseTime> searchPulseTime(long pulse, DataPath dataPath) {
        return Mono.defer(() -> {
            PulseTime ret = new PulseTime();
            ret.dataPath = dataPath;
            try {
                Chan chan = Chan.of(Files.newByteChannel(dataPath.path));
                return findFirstChunk(chan)
                .flatMap(k -> {
                    ret.firstPos = k.pos;
                    ret.append(k);
                    return findChunkLength(k.chan, k.pos);
                })
                .flatMap(kr -> {
                    try (SeekableByteChannel chn = kr.chan.take()) {
                        try {
                            ByteBuffer buf = ByteBuffer.allocate(1024);
                            ret.chunkLen = kr.len;
                            long v = kr.len;
                            long clen = chn.size();
                            long j = ret.firstPos;
                            long k = j + ((clen - j) / v) * v;
                            long v2 = 2 * v;
                            long r = -1;
                            while (k - j >= v2) {
                                long m = j + (k - j) / v / 2 * v;
                                chn.position(m);
                                buf.clear();
                                tryRead(chn, buf);
                                ret.reads += 1;
                                if (buf.remaining() < v) {
                                    return Mono.error(new IOException("read error"));
                                }
                                int len1 = buf.getInt();
                                if (len1 != v) {
                                    return Mono.error(new RuntimeException("len mismatch"));
                                }
                                long ttl = buf.getLong();
                                long ts = buf.getLong();
                                long pu = buf.getLong();
                                if (ts < 0) {
                                    return Mono.error(new RuntimeException("unexpected ts value"));
                                }
                                if (pu < 0) {
                                    return Mono.error(new RuntimeException("unexpected pu value"));
                                }
                                if (pu == pulse) {
                                    r = ts;
                                    break;
                                }
                                if (pu >= pulse) {
                                    k = m;
                                }
                                else {
                                    j = m;
                                }
                            }
                            if (r >= 0) {
                                ret.ts = r;
                            }
                            return Mono.just(ret);
                        }
                        catch (IOException e) {
                            return Mono.error(e);
                        }
                    }
                    catch (IOException e) {
                        return Mono.error(e);
                    }
                });
            }
            catch (IOException e) {
                return Mono.error(e);
            }
        });
    }

    Mono<List<String>> distinctChannels() {
        try (Connection conn = connection().block()) {
            try (PreparedStatement st = conn.prepareStatement("select distinct channel from map_pulse_files order by channel")) {
                try (ResultSet res = st.executeQuery()) {
                    List<String> ret = new ArrayList<>();
                    while (res.next()) {
                        ret.add(res.getString(1));
                    }
                    return Mono.just(ret);
                }
            }
            catch (SQLException e) {
                throw new RuntimeException();
            }
        }
        catch (SQLException e) {
            return Mono.error(e);
        }
    }

    Mono<Void> markClosed() {
        return Mono.defer(() -> {
            List<String> dcs = distinctChannels().block();
            //LOGGER.info("dcs  {}", dcs);
            try (Connection conn = connection().block()) {
                dcs.forEach(ch -> {
                    localSplits().forEach(split -> {
                        try (PreparedStatement st = conn.prepareStatement("with q1 as (select channel, split, timebin from map_pulse_files where channel = ? and split = ? order by timebin desc offset 2) update map_pulse_files t2 set closed = 1 from q1 where t2.channel = q1.channel and t2.split = q1.split and t2.timebin = q1.timebin")) {
                            st.setString(1, ch);
                            st.setInt(2, split);
                            st.execute();
                        }
                        catch (SQLException e) {
                            throw new RuntimeException();
                        }
                    });
                });
                return Mono.empty();
            }
            catch (SQLException e) {
                return Mono.error(e);
            }
        });
    }

    List<Integer> localSplits() {
        return api1.conf.nodes.stream()
        .filter(k -> k.host.startsWith(api1.conf.canonicalHostname) || api1.conf.canonicalHostname.startsWith(k.host))
        .flatMap(k -> k.splits.stream())
        .collect(Collectors.toList());
    }

    String localSplitsString() {
        String ret = localSplits().stream()
        .map(Object::toString)
        .reduce("", (a, x) -> {
            if (a.length() == 0) {
                return x;
            }
            else {
                return a + ", " + x;
            }
        });
        //LOGGER.info("local splits  {}  {}", api1.canonicalHostname, ret);
        return ret;
    }

    @Scheduled(fixedDelay = 3000)
    void doIndex() {
        try {
            mapIndex()
            .then(markClosed())
            .delaySubscription(Duration.ofMillis(new Random().nextInt(700)))
            .block();
        }
        catch (Throwable e) {
            if (api1.shuttingDown.get() == 0) {
                throw e;
            }
        }
    }


    @Autowired
    void setAPIb(API_1_0_1 k) {
        api1 = k;
    }

    API_1_0_1 api1;

}
