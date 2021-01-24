package ch.psi.daq.retrieval.controller;

import ch.psi.daq.retrieval.finder.StorageSettings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

@RestController
public class Testing {
    final API_1_0_1 api1;

    public Testing(API_1_0_1 api1) {
        this.api1 = api1;
    }

    @GetMapping(path = "/map")
    public Flux<String> getMap(ServerWebExchange exchange) throws IOException {
        List<String> channels = List.of("SLAAR11-LSCP1-FNS:CH0:VAL_GET");
        StorageSettings sts = new StorageSettings(Path.of(api1.conf.databufferBaseDir), api1.conf.databufferKeyspacePrefix);
        //return DatafileScan.scanForMapping(sts, channels, exchange.getResponse().bufferFactory());
        // TODO
        return Flux.empty();
    }

}
