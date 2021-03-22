package ch.psi.daq.retrieval.utils;

import ch.qos.logback.classic.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DataBufferUtils;
import reactor.core.publisher.SynchronousSink;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.function.Consumer;

public class ReadGenerator implements Consumer<SynchronousSink<DataBuffer>> {
    static final Logger LOGGER = (Logger) LoggerFactory.getLogger(ReadGenerator.class.getSimpleName());

    public ReadGenerator(ReadableByteChannel channel, DataBufferFactory dataBufferFactory, int bufferSize) {
        this.channel = channel;
        this.dataBufferFactory = dataBufferFactory;
        this.bufferSize = bufferSize;
    }

    @Override
    public void accept(SynchronousSink<DataBuffer> sink) {
        boolean release = true;
        DataBuffer buf = dataBufferFactory.allocateBuffer(bufferSize);
        try {
            int read;
            if (bufferSize < 1) {
                LOGGER.error("BAD BUFFERSIZE {}", bufferSize);
                sink.complete();
            }
            else {
                ByteBuffer byteBuffer = buf.asByteBuffer(0, buf.capacity());
                if (byteBuffer.remaining() < 1) {
                    LOGGER.error("no remaining in byte buffer");
                    sink.complete();
                }
                else {
                    read = channel.read(byteBuffer);
                    if (read > 0) {
                        buf.writePosition(read);
                        release = false;
                        sink.next(buf);
                    }
                    else if (read == 0) {
                        LOGGER.error("read 0 bytes");
                        sink.complete();
                    }
                    else if (read == -1) {
                        sink.complete();
                    }
                    else {
                        LOGGER.error("read returned {}", read);
                        sink.complete();
                    }
                }
            }
        }
        catch (IOException ex) {
            sink.error(ex);
        }
        finally {
            if (release) {
                DataBufferUtils.release(buf);
            }
        }
    }

    final ReadableByteChannel channel;
    final DataBufferFactory dataBufferFactory;
    final int bufferSize;

}
