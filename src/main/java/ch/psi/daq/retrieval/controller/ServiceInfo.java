package ch.psi.daq.retrieval.controller;

import ch.psi.daq.retrieval.ReqCtx;
import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebExchange;

import java.io.IOException;
import java.io.InputStream;
import java.lang.management.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@RestController
public class ServiceInfo {
    static final Logger LOGGER = (Logger) LoggerFactory.getLogger(ServiceInfo.class);
    @Autowired
    API_1_0_1 api101;

    @GetMapping(path = "/stats/version")
    public String version() throws IOException {
        Properties props = new Properties();
        props.load(new DefaultResourceLoader().getResource("info.properties").getInputStream());
        return props.getProperty("appVersion") + " (" + props.getProperty("buildDateTime") + ")";
    }

    @GetMapping(path = "/logtest")
    public String version(ServerWebExchange exchange) {
        ReqCtx reqctx = ReqCtx.fromRequest(exchange.getRequest());
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

}
