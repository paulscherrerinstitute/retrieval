package ch.psi.daq.retrieval.controller;

import ch.psi.daq.retrieval.ReqCtx;
import ch.psi.daq.retrieval.config.ConfigurationDatabase;
import ch.psi.daq.retrieval.error.RetrievalException;
import ch.qos.logback.classic.Logger;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RestController
public class MapPulseTime {
    static final Logger LOGGER = (Logger) LoggerFactory.getLogger(MapPulseTime.class.getSimpleName());

    @GetMapping(path = "/api/1/map/pulse/local/{pulse}")
    public Mono<Deque<Long>> mapPulseLocal(ServerWebExchange exchange, @PathVariable long pulse) {
        ReqCtx reqCtx = ReqCtx.fromRequest(exchange);
        return mapPulse(pulse)
        .map(k -> k.ts)
        .doOnError(e -> {
            LOGGER.error("ERROR  {}", e.toString());
            throw new RuntimeException(e);
        })
        .subscribeOn(Schedulers.boundedElastic());
    }

    @GetMapping(path = "/api/1/map/pulse/locallog/{pulse}")
    public Mono<StringBuilder> mapPulseLocalLog(ServerWebExchange exchange, @PathVariable long pulse) {
        ReqCtx reqCtx = ReqCtx.fromRequest(exchange);
        return mapPulse(pulse)
        .map(k -> k.sb)
        .doOnError(e -> {
            LOGGER.error("ERROR  {}", e.toString());
            throw new RuntimeException(e);
        })
        .subscribeOn(Schedulers.boundedElastic());
    }

    // TODO factor endpoint so that I can also query the ranked list

    @GetMapping(path = "/api/1/map/pulse/{pulse}")
    public Mono<String> mapPulse(ServerWebExchange exchange, @PathVariable long pulse) {
        return Flux.fromIterable(api1.conf.nodes)
        .flatMap(node -> {
            return WebClient.create().get().uri(String.format("http://%s:%d/api/1/map/pulse/local/%d", node.host, node.port, pulse))
            .retrieve().toEntity(JsonNode.class)
            .map(res -> {
                JsonNode body = JsonNodeFactory.instance.arrayNode();
                if (res.hasBody()) {
                    body = res.getBody();
                }
                //LOGGER.info("Got remote response  {}  {}", node.host, body);
                return body;
            })
            .flatMapIterable(k -> {
                if (k.isArray()) {
                    List<Long> a = new ArrayList<>();
                    for (int i = 0; i < k.size(); i += 1) {
                        a.add(k.get(i).asLong());
                    }
                    return a;
                }
                else {
                    return List.of();
                }
            });
        }, 20, 20)
        .reduce(new TreeMap<Long, Long>(), (a, x) -> {
            if (a.containsKey(x)) {
                a.put(x, a.get(x) + 1);
            }
            else {
                a.put(x, 1L);
            }
            return a;
        })
        .<String>map(k -> {
            List<S> all = new ArrayList<>();
            k.forEach((a, b) -> {
                S s = new S();
                s.a = a;
                s.b = b;
                all.add(s);
            });
            if (all.size() <= 0) {
                return "-1";
            }
            all.sort((a, b) -> -Long.compare(a.b, b.b));
            long ret = all.get(0).a;
            return Long.toString(ret);
            //return (List<Long>) new ArrayList<>(k.keySet());
        })
        .subscribeOn(Schedulers.boundedElastic());
    }

    static class S {
        long a;
        long b;
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
                .flatMap(q -> searchPulseTime(pulse, q), 4)
                .filter(q -> q.ts >= 0)
                .map(q -> {
                    ret.println("hit   %d   in %s", q.ts, q.dataPath.path);
                    return q.ts;
                });
            })
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
                LOGGER.error("{}", e.toString());
                throw new RuntimeException(e);
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
        try {
            ConfigurationDatabase c = api1.conf.database;
            return Mono.just(DriverManager.getConnection(String.format("jdbc:postgresql://%s:%d/%s", c.host, c.port, c.database), c.username, c.password));
        }
        catch (SQLException e) {
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
        sqlExec(conn, "create table map_pulse_channels (name text, tbmax int)");
        sqlExec(conn, "create table map_pulse_files (channel text not null, split int not null, timebin int not null, closed int not null default 0, pulse_min int8 not null, pulse_max int8 not null)");
        sqlExec(conn, "create unique index map_pulse_files_ix1 on map_pulse_files (channel, split, timebin)");
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
        Instant tsBeg = Instant.ofEpochSecond(10000);
        Instant tsEnd = Instant.ofEpochSecond(2100000000);
        List<Integer> splits = List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14);
        return api1.queryData.baseDirFinder.findMatchingDataFiles(reqCtx, channelName, tsBeg, tsEnd, splits, reqCtx.bufCtx.bufFac)
        .flatMapMany(mres -> {
            return Flux.fromStream(() -> {
                return mres.keyspaces.stream().flatMap(ks -> {
                    return ks.splits.stream().flatMap(sp -> {
                        return sp.timeBins.stream().map(tb -> {
                            return String.format("%s  ks %s  sp %d  bs %d  tb %d  index %5s", mres.channelName, ks.ksp, sp.split, tb.binSize, tb.timeBin, tb.hasIndex);
                        });
                    });
                });
            });
        });
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
        return api1.queryData.baseDirFinder.findMatchingDataFiles(reqCtx, channelName, tsBeg, tsEnd, splits, reqCtx.bufCtx.bufFac)
        .flatMapMany(mres -> {
            return Flux.fromStream(() -> {
                return mres.keyspaces.stream().flatMap(ks -> {
                    return ks.splits.stream().flatMap(sp -> {
                        return sp.timeBins.stream().map(tb -> {
                            return ks.filePath(tb, sp);
                            //String.format("%s/config/%s/latest/00000_Config", channel.base.baseDir, channel.name)
                        });
                    });
                });
            });
        });
    }

    static class TimerScan extends LogRes<TimerScan> {
    }

    Mono<TimerScan> scanTimers() {
        /*
        Examples:
        SINEG01-RLLE-STA:MASTER-EVRPULSEID
        SINSB01-RLLE-STA:MASTER-EVRPULSEID
        */
        List<String> sections = List.of("SINEG01", "SINSB01", "SINSB02", "SINSB03", "SINSB04", "SINXB01");
        List<String> suffix = List.of("MASTER", "SLAVE");
        List<String> channels = sections.stream()
        .flatMap(sec -> suffix.stream().map(suf -> sec + "-RLLE-STA:" + suf + "-EVRPULSEID"))
        .limit(12)
        .collect(Collectors.toList());
        Connection conn = connection().block();
        TimerScan ret = new TimerScan();
        return Flux.fromIterable(channels)
        .flatMap(k -> scanChannel(k).onErrorResume(e -> Mono.empty()), 4)
        .doOnNext(k -> {
            k.bounds.stream().forEach(q -> {
                ret.println("bounds  %d  %d  %d  %d  %s", q.pulseFirst, q.pulseLast, q.dataPath.tb, q.dataPath.split, q.dataPath.channel);
            });
        })
        .concatMap(k -> updateDb(k, conn))
        .doFinally(k -> closeConn(conn))
        .then(Mono.just(ret));
    }

    Mono<Void> updateDb(ChannelScan channelScan, Connection conn) {
        return Mono.fromCallable(() -> {
            channelScan.bounds.forEach(bound -> {
                try (PreparedStatement st = conn.prepareStatement("insert into map_pulse_files (channel, split, timebin, pulse_min, pulse_max) values (?, ?, ?, ?, ?) on conflict (channel, split, timebin) do update set pulse_min=?, pulse_max=?")) {
                    st.setString(1, bound.dataPath.channel);
                    st.setInt(2, bound.dataPath.split);
                    st.setInt(3, (int) bound.dataPath.tb);
                    st.setLong(4, bound.pulseFirst);
                    st.setLong(5, bound.pulseLast);
                    st.setLong(6, bound.pulseFirst);
                    st.setLong(7, bound.pulseLast);
                    st.execute();
                }
                catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });
            return null;
        });
    }

    static class ChannelScan extends LogRes<ChannelScan> {
        String name;
        Deque<DatafileBounds> bounds = new ArrayDeque<>();
    }

    Mono<ChannelScan> scanChannel(String channel) {
        List<Integer> splits = api1.conf.nodes.stream()
        .filter(k -> k.host.startsWith(api1.canonicalHostname) || api1.canonicalHostname.startsWith(k.host))
        .flatMap(k -> k.splits.stream())
        .collect(Collectors.toList());
        ChannelScan ret = new ChannelScan();
        ret.name = channel;
        return Flux.fromIterable(splits)
        .concatMap(sp -> dataPathsForChannelSplit(channel, sp), 0)
        .concatMap(k -> {
            ret.append(k);
            return previewFiles(k)
            .doOnNext(ret::append);
        }, 0)
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
                    ret.println("can not even read len  %s");
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
                    ret.println("can not even read one chunk header  %s\n");
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
                ret.println("%s", e.toString());
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
        .filter(k -> k.host.startsWith(api1.canonicalHostname) || api1.canonicalHostname.startsWith(k.host))
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
        mapIndex()
        .then(markClosed())
        .delaySubscription(Duration.ofMillis(new Random().nextInt(700)))
        .block();
    }


    @Autowired
    void setAPIb(API_1_0_1 k) {
        api1 = k;
    }

    API_1_0_1 api1;

}
