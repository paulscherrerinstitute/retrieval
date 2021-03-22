package ch.psi.daq.retrieval.eventmap.unpack;

import ch.psi.daq.retrieval.eventmap.basic.BasicResult;
import ch.psi.daq.retrieval.eventmap.basic.Chunk;
import ch.psi.daq.retrieval.merger.Releasable;
import ch.psi.daq.retrieval.utils.Tools;
import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;

public class UnpackMap implements Function<BasicResult, UnpackResult> {
    static final Logger LOGGER = (Logger) LoggerFactory.getLogger(UnpackMap.class.getSimpleName());
    UnpackParams params;
    State state;

    enum State {
        EXPECT_HEADER_A,
        EXPECT_HEADER_B,
        EXPECT_HEADER_C,
        EXPECT_HEADER_D,
        EXPECT_BLOBS,
        EXPECT_SECOND_LENGTH,
        TERM,
    }

    public UnpackMap(UnpackParams params) {
        this.params = params;
        this.state = State.EXPECT_HEADER_A;
    }

    public static Flux<UnpackResult> trans(Flux<BasicResult> fl, UnpackParams params) {
        UnpackMap mapper = new UnpackMap(params);
        return fl.map(mapper)
        .takeWhile(q -> Tools.passOrRelease(q, UnpackResult::notTerm))
        .doFinally(k -> mapper.release());
    }

    @Override
    public UnpackResult apply(BasicResult inp) {
        if (inp.isTerm()) {
            throw new RuntimeException("logic");
        }
        else {
            if (state == State.TERM) {
                inp.release();
                return UnpackResult.create();
            }
            else {
                return applyInput(inp);
            }
        }
    }

    UnpackResult applyInput(BasicResult inp) {
        UnpackResult res = new UnpackResult();
        int ix = 0;
        while (state != State.TERM && ix < inp.chunks.size()) {
            applyChunk(res, inp.chunks.get(ix));
            ix += 1;
        }
        inp.release();
        return res;
    }

    void applyChunk(UnpackResult res, Chunk chunk) {
        if (state == State.EXPECT_HEADER_A) {
        }
        else if (state == State.EXPECT_HEADER_B) {
        }
        else if (state == State.EXPECT_HEADER_C) {
        }
        else if (state == State.EXPECT_HEADER_D) {
        }
        else if (state == State.EXPECT_BLOBS) {
        }
        else if (state == State.EXPECT_SECOND_LENGTH) {
        }
        else if (state == State.TERM) {
        }
        else {
            // TODO count
        }
    }

    public void release() {}

}
