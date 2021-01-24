package ch.psi.daq.retrieval.controller;

import ch.psi.daq.retrieval.BufCtx;
import io.netty.buffer.PooledByteBufAllocator;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.NettyDataBufferFactory;
import reactor.core.publisher.SynchronousSink;

import java.nio.ByteBuffer;

class GenSt {
    static GenSt create(BufCtx bufCtx) {
        GenSt ret = new GenSt();
        ret.bufCtx = bufCtx;
        return ret;
    }

    GenSt next(SynchronousSink<DataBuffer> sink) {
        DataBuffer dbuf = fac2.allocateBuffer(bufCtx.bufferSize);
        ByteBuffer buf = dbuf.asByteBuffer(0, dbuf.capacity());
        byte status = 0;
        byte severity = 0;
        int optLen = -1;
        byte flags = (byte) 0xd0;
        byte dtype = 11;
        int posLen = buf.position();
        buf.putInt(0xcafecafe);
        buf.putLong(123L);
        buf.putLong(123L);
        buf.putLong(123L);
        buf.putLong(123L);
        buf.put(status);
        buf.put(severity);
        buf.putInt(optLen);
        buf.put(flags);
        buf.put(dtype);
        byte method = 0;
        buf.put(method);
        buf.put((byte) 1);
        buf.putInt(124);
        buf.position(buf.position() + 2178);
        int len = 4 + buf.position() - posLen;
        buf.putInt(posLen, len);
        buf.putInt(len);
        dbuf.readPosition(0);
        dbuf.writePosition(buf.position());
        sink.next(dbuf);
        return this;
    }

    static void release(GenSt st) {
    }

    BufCtx bufCtx;
    DataBufferFactory fac2 = new NettyDataBufferFactory(PooledByteBufAllocator.DEFAULT);

}
