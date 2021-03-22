package ch.psi.daq.retrieval.eventmap.unpack.scalarnum;

import ch.psi.daq.retrieval.bytes.BufCont;
import ch.psi.daq.retrieval.eventmap.basic.BasicResult;
import ch.psi.daq.retrieval.eventmap.basic.Chunk;
import ch.psi.daq.retrieval.eventmap.basic.ChunkBlob;
import ch.psi.daq.retrieval.eventmap.basic.ChunkHeaderA;
import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.util.function.Function;

public class UnpackScalarNum<T extends Num<T>> implements Function<BasicResult, UnpackScalarNumRes<T>> {
    static final Logger LOGGER = (Logger) LoggerFactory.getLogger(UnpackScalarNum.class.getSimpleName());

    public static <T extends Num<T>> Flux<UnpackScalarNumRes<T>> trans(Flux<BasicResult> fl, T num) {
        UnpackScalarNum<T> mapper = new UnpackScalarNum<>(num);
        return fl.map(mapper)
        .doFinally(k -> mapper.release());
    }

    public UnpackScalarNum(T num) {
        this.num = num;
        state = State.EXPECT_HEADER_A;
    }

    enum State {
        EXPECT_HEADER_A,
        EXPECT_HEADER_B,
        EXPECT_HEADER_C,
        EXPECT_HEADER_D,
        EXPECT_BLOBS,
        EXPECT_SECOND_LENGTH,
        TERM,
    }

    @Override
    public UnpackScalarNumRes<T> apply(BasicResult inp) {
        return applyInput(inp);
    }

    UnpackScalarNumRes<T> applyInput(BasicResult inp) {
        UnpackScalarNumRes<T> res = new UnpackScalarNumRes<>(num);
        int ix = 0;
        while (state != State.TERM && ix < inp.chunks.size()) {
            applyChunk(res, inp, inp.chunks.get(ix));
            ix += 1;
        }
        inp.release();
        return res;
    }

    void applyChunk(UnpackScalarNumRes<T> res, BasicResult inp, Chunk chunk) {
        if (state == State.EXPECT_HEADER_A) {
            if (chunk instanceof ChunkHeaderA) {
                ChunkHeaderA ch = (ChunkHeaderA) chunk;
                ts = ch.ts;
                pulse = ch.pulse;
                state = State.EXPECT_HEADER_B;
            }
            else {
                throw new RuntimeException("logic");
            }
        }
        else if (state == State.EXPECT_HEADER_B) {
            state = State.EXPECT_HEADER_C;
        }
        else if (state == State.EXPECT_HEADER_C) {
            state = State.EXPECT_HEADER_D;
        }
        else if (state == State.EXPECT_HEADER_D) {
            state = State.EXPECT_BLOBS;
        }
        else if (state == State.EXPECT_BLOBS) {
            if (chunk instanceof ChunkBlob) {
                ChunkBlob blob = (ChunkBlob) chunk;
                BufCont bc = inp.bufs[blob.bufIx];
                //LOGGER.info("ChunkBlob  {}  {}  {}", blob.pos, blob.len, blob.pos + blob.len <= bc.bufferRef().capacity());
                res.consume(ts, pulse, bc.bufferRef().slice(blob.pos, blob.len));
                state = State.EXPECT_SECOND_LENGTH;
            }
            else {
                throw new RuntimeException("logic");
            }
        }
        else if (state == State.EXPECT_SECOND_LENGTH) {
            state = State.EXPECT_HEADER_A;
        }
        else if (state == State.TERM) {
        }
        else {
            // TODO count
        }
    }

    public void release() {}

    final T num;
    State state;
    long ts;
    long pulse;

}
