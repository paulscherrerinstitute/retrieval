package ch.psi.daq.retrieval.controller.octets;

import ch.psi.daq.retrieval.bytes.BufCont;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Optional;

public class BufAcc {

    public static Flux<BufCont> trans(Flux<BufCont> fl) {
        BufAcc acc = new BufAcc();
        return fl.map(k -> {
            Optional<BufCont> ret = Optional.empty();
            if (acc.cur == null) {
                acc.cur = BufCont.allocate(DefaultDataBufferFactory.sharedInstance, 1024 * 512, BufCont.Mark.Acc1);
            }
            else {
                DataBuffer cbuf = acc.cur.bufferRef();
                if (cbuf.writableByteCount() < k.bufferRef().readableByteCount()) {
                    ret = Optional.of(acc.cur);
                    acc.cur = null;
                    acc.cur = BufCont.allocate(DefaultDataBufferFactory.sharedInstance, 1024 * 512, BufCont.Mark.Acc2);
                }
            }
            DataBuffer cbuf = acc.cur.bufferRef();
            DataBuffer kbuf = k.bufferRef();
            if (cbuf.writableByteCount() < kbuf.readableByteCount()) {
                k.close();
                throw new RuntimeException("logic");
            }
            cbuf.write(kbuf);
            kbuf = null;
            k.close();
            return ret;
        })
        .filter(Optional::isPresent)
        .map(Optional::get)
        .concatWith(Mono.defer(() -> {
            if (acc.cur != null) {
                BufCont ret = acc.cur;
                acc.cur = null;
                return Mono.just(ret);
            }
            else {
                return Mono.empty();
            }
        }))
        .doFinally(k -> {
            if (acc.cur != null) {
                acc.cur.close();
                acc.cur = null;
            }
        });
    }

    BufCont cur;

}
