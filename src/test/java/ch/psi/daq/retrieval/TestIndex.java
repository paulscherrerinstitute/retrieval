package ch.psi.daq.retrieval;

import com.google.common.io.BaseEncoding;
import org.junit.Test;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;

import static ch.psi.daq.retrieval.Index.show;
import static ch.psi.daq.retrieval.Index.cmp;
import static org.junit.Assert.*;

public class TestIndex {

    static final int N = 16;

    /*
    test data file: 0000000000003600000_00000_Data_Index   (47f91db6b32f1ab24b0afbd592667259a660727c)
    begin: 15f0659e5ac18ace
    end:   15f068e489881ee2
     5000  15f065fb5804e173
    10000  15f066585407c932
    23000  15f0674a320e8d77
    45000  15f068e3de0c9456
    */

    @Test
    public void find5000() {
        byte[] tgt = BaseEncoding.base16().lowerCase().decode("15f065fb5804e173");
        assertEquals(5000 * 16,
        monoTestIndex()
        .doOnNext(TestIndex::testCmp)
        .map(x -> Index.findGE(tgt, x))
        .block().i
        );
    }

    @Test
    public void find10000() {
        byte[] tgt = BaseEncoding.base16().lowerCase().decode("15f066585407c932");
        assertEquals(10000 * 16,
        monoTestIndex()
        .doOnNext(TestIndex::testCmp)
        .map(x -> Index.findGE(tgt, x))
        .block().i
        );
    }

    @Test
    public void find23000() {
        byte[] tgt = BaseEncoding.base16().lowerCase().decode("15f0674a320e8d77");
        assertEquals(23000 * 16,
        monoTestIndex()
        .doOnNext(TestIndex::testCmp)
        .map(x -> Index.findGE(tgt, x))
        .block().i
        );
    }

    @Test
    public void find45000() {
        byte[] tgt = BaseEncoding.base16().lowerCase().decode("15f068e3de0c9456");
        assertEquals(45000 * 16,
        monoTestIndex()
        .doOnNext(TestIndex::testCmp)
        .map(x -> Index.findGE(tgt, x))
        .block().i
        );
    }

    @Test
    public void findBegin() {
        byte[] tgt = BaseEncoding.base16().lowerCase().decode("15f0659e5ac18ace");
        assertEquals(0,
        monoTestIndex()
        .doOnNext(TestIndex::testCmp)
        .map(x -> Index.findGE(tgt, x))
        .block().i
        );
    }

    @Test
    public void findEnd() {
        byte[] tgt = BaseEncoding.base16().lowerCase().decode("15f068e489881ee2");
        assertEquals(45036 * 16,
        monoTestIndex()
        .doOnNext(TestIndex::testCmp)
        .map(x -> Index.findGE(tgt, x))
        .block().i
        );
    }

    @Test
    public void findPastEnd() {
        byte[] tgt = BaseEncoding.base16().lowerCase().decode("15f068e489881ee3");
        assertEquals(-1,
        monoTestIndex()
        .doOnNext(TestIndex::testCmp)
        .map(x -> Index.findGE(tgt, x))
        .block().i
        );
    }

    @Test
    public void findBeforeBegin() {
        byte[] tgt = BaseEncoding.base16().lowerCase().decode("15f068e489881ee3");
        assertEquals(-1,
        monoTestIndex()
        .doOnNext(TestIndex::testCmp)
        .map(x -> Index.findGE(tgt, x))
        .block().i
        );
    }

    @Test
    public void printLoc() {
        monoTestIndex()
        .doOnNext(buf -> {
            assertEquals("15f0674a320e8d77", show(buf, 23000 * 16));
        })
        .block();
    }

    static Mono<byte[]> monoTestIndex() {
        Path path = Path.of("test/data/47f91db6b32f1ab24b0afbd592667259a660727c");
        return Index.openIndex(path);
    }

    static void testCmp(byte[] a) {
        int n = a.length / N;
        for (int k = 0; k < n-N; k+=N) {
            int v = cmp(a, k, a, k+N);
            if (v != -1) {
                show(a, k);
                show(a, k+N);
            }
            assertEquals(-1, v);
            assertEquals(+1, cmp(a, k+N, a, k));
            assertEquals(0, cmp(a, k, a, k));
        }
    }

    @Test
    public void smallIndex() throws IOException {
        Files.createDirectories(Path.of("tmp"));
        WritableByteChannel wr = Files.newByteChannel(Path.of("tmp/index1"), EnumSet.of(StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE));
        ByteBuffer buf = ByteBuffer.allocate(1);
        buf.put((byte) 1);
        buf.flip();
        wr.write(buf);
        wr.close();
        try {
            Index.openIndex(Path.of("tmp/index1")).block();
            assertTrue("Expected exception", false);
        }
        catch (RuntimeException e) {
        }
    }

    @Test
    public void malformedIndex() throws IOException {
        Files.createDirectories(Path.of("tmp"));
        WritableByteChannel wr = Files.newByteChannel(Path.of("tmp/index2"), EnumSet.of(StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE));
        ByteBuffer buf = ByteBuffer.allocate(19);
        buf.putShort((short) 0);
        buf.putLong(1L);
        buf.putLong(2L);
        buf.put((byte) 2);
        buf.flip();
        wr.write(buf);
        wr.close();
        try {
            Index.openIndex(Path.of("tmp/index2")).block();
            assertTrue("Expected exception", false);
        }
        catch (RuntimeException e) {
        }
    }

    @Test
    public void keyNotFound() throws IOException {
        Files.createDirectories(Path.of("tmp"));
        WritableByteChannel wr = Files.newByteChannel(Path.of("tmp/index3"), EnumSet.of(StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE));
        ByteBuffer buf = ByteBuffer.allocate(1024);
        buf.putShort((short) 0);
        buf.putLong(100L);
        buf.putLong(101L);
        buf.putLong(200L);
        buf.putLong(201L);
        buf.putLong(300L);
        buf.putLong(301L);
        buf.flip();
        wr.write(buf);
        wr.close();
        byte[] ix = Index.openIndex(Path.of("tmp/index3")).block();
        Index.FindResult res;
        res = Index.findGEByLong(200, ix);
        assertEquals(200, res.k);
        assertEquals(201, res.v);
        res = Index.findGEByLong(201, ix);
        assertTrue(res.isSome());
        res.toString();
        assertEquals(300, res.k);
        assertEquals(301, res.v);
        res = Index.findGEByLong(301, ix);
        res.toString();
        assertEquals(-1, res.k);
        assertEquals(0, res.v);
    }

    @Test
    public void indexFileTooLarge() throws IOException {
        Files.createDirectories(Path.of("tmp"));
        WritableByteChannel wr = Files.newByteChannel(Path.of("tmp/index4"), EnumSet.of(StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE));
        byte[] a = new byte[1024 * 1024];
        ByteBuffer buf = ByteBuffer.wrap(a);
        for (int i1 = 0; i1 < 31; i1 += 1) {
            wr.write(buf);
            buf.flip();
        }
        a = new byte[2];
        wr.write(ByteBuffer.wrap(a));
        wr.close();
        try {
            Index.openIndex(Path.of("tmp/index4")).block();
            fail("expected exception");
        }
        catch (RuntimeException e) {
        }
    }

    @Test
    public void malformedIndexData() {
        try {
            byte[] a = new byte[17];
            ByteBuffer buf = ByteBuffer.wrap(a);
            buf.putLong(0);
            buf.putLong(0);
            buf.put((byte) 1);
            buf.flip();
            assertEquals(17, a.length);
            assertEquals(17, buf.array().length);
            Index.findGEByLong(0, a);
            fail("Expected exception");
        }
        catch (RuntimeException e) {
        }
    }

    @Test
    public void emptyIndexData() {
        byte[] a = new byte[0];
        assertEquals(-1, Index.findGEByLong(0, a).k);
    }

}
