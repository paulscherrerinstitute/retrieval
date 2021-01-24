package ch.psi.daq.retrieval.status;

import ch.psi.daq.retrieval.ReqCtx;
import ch.qos.logback.classic.Logger;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class RequestStatusFetch {
    static final Logger LOGGER = (Logger) LoggerFactory.getLogger(RequestStatusFetch.class.getSimpleName());

    public static Mono<Void> getRequestStatus(RequestStatusBoard requestStatusBoard, ReqCtx reqCtx, String host, int port, String reqId) {
        //LOGGER.info("{}  getRequestStatus  from {}", reqCtx, host);
        String requestStatusUrl = String.format("http://%s:%d/api/1/requestStatus/%s", host, port, reqId);
        return WebClient.create().get().uri(requestStatusUrl)
        .exchangeToMono(res -> {
            if (res.statusCode() != HttpStatus.OK) {
                LOGGER.error("{}  getRequestStatus  for remote req {}  got http status {}", reqCtx, reqId, res.statusCode());
                RequestStatus.Error e = new RequestStatus.Error(String.format("http %s", res.statusCode()));
                return Mono.just(new RequestStatusResult(e));
            }
            return res.bodyToMono(ByteBuffer.class)
            .map(buf -> {
                String s1 = StandardCharsets.UTF_8.decode(buf).toString();
                try {
                    return new RequestStatusResult((new ObjectMapper()).readValue(s1, RequestStatus.class));
                }
                catch (Throwable e2) {
                    LOGGER.error("{}  getRequestStatus  for {}  can not parse  {}  {}", reqCtx, reqId, e2.toString(), s1);
                    RequestStatus.Error e = new RequestStatus.Error("parse error");
                    return new RequestStatusResult(e);
                }
            });
        })
        .doOnError(e -> {
            LOGGER.error("{}  error from WebClient getRequestStatus  from {}", reqCtx, host);
        })
        .onErrorResume(e -> Mono.empty())
        .doOnNext(k -> {
            if (k.isError()) {
                LOGGER.info("{}  adding error  {}", reqCtx, k.error());
                requestStatusBoard.getOrCreate(reqCtx).addError(k.error());
            }
            else if (k.isStatus()) {
                requestStatusBoard.getOrCreate(reqCtx).addSubRequestStatus(k.status());
            }
            else {
                LOGGER.error("unhandled status type");
            }
        })
        .then();
    }

}
