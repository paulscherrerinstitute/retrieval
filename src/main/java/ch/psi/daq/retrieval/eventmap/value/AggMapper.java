package ch.psi.daq.retrieval.eventmap.value;

import ch.psi.daq.retrieval.pod.api1.Event;
import ch.psi.daq.retrieval.pod.api1.EventAggregated;
import ch.psi.daq.retrievalutils.aggregation.Aggregator;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;

import java.io.IOException;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

public class AggMapper {
    Aggregator agg;
    JsonGenerator jgen;
    JgenState jst = new JgenState();
    BinFind binFind;
    static long INIT = Long.MIN_VALUE;
    AtomicLong binCurrent = new AtomicLong(INIT);
    AtomicLong binTs = new AtomicLong();
    AtomicLong binPulse = new AtomicLong();
    AtomicLong evCount = new AtomicLong();
    List<AggFunc> aggs;
    OutputBuffer outbuf;

    public AggMapper(BinFind binFind, List<AggFunc> aggs, DataBufferFactory bufFac) {
        try {
            outbuf = new OutputBuffer(bufFac);
            JsonFactory jfac = new JsonFactory();
            jgen = jfac.createGenerator(outbuf);
            jgen.setCodec(new ObjectMapper());
            // Sending the response as a plain array instead of wrapping the array of channels into an object-
            // member is specifically demanded.
            // https://jira.psi.ch/browse/CTRLIT-7984
            jgen.writeStartArray();
            this.binFind = binFind;
            this.aggs = aggs;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public List<DataBuffer> map(MapJsonResult res) {
        try {
            for (MapJsonItem item : res.getItems()) {
                item.mapOutput(this, jgen, jst);
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        res.release();
        return outbuf.getPending();
    }

    public List<DataBuffer> finalResult() {
        try {
            if (jst.inChannel) {
                if (binFind != null) {
                    writeCurrent();
                }
            }
            jst.beOutOfChannel(jgen);
            jgen.writeEndArray();
            jgen.close();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        return outbuf.getPending();
    }

    void writeCurrent() throws IOException {
        EventAggregated eva = new EventAggregated(binTs.get(), binPulse.get(), evCount.get());
        eva.data = new TreeMap<>();
        for (AggFunc f : aggs) {
            eva.data.put(f.name(), f.result());
        }
        jgen.writeObject(eva);
    }

    void processEvent(MapJsonEvent ev) {
        if (binFind != null) {
            int bin = binFind.find(ev.ts);
            if (binCurrent.get() == INIT) {
                binCurrent.set(bin);
                binTs.set(ev.ts);
                binPulse.set(ev.pulse);
            }
            else if (bin != binCurrent.get()) {
                try {
                    writeCurrent();
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
                binCurrent.set(bin);
                binTs.set(ev.ts);
                binPulse.set(ev.pulse);
                evCount.set(0);
                for (AggFunc f : aggs) {
                    f.reset();
                }
            }
            evCount.getAndAdd(1);
            for (AggFunc f : aggs) {
                f.sink(ev.data);
            }
        }
        else {
            Event eva = new Event(binTs.get(), binPulse.get());
            eva.data = ev.data;
            try {
                jgen.writeObject(eva);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void release() {
        outbuf.release();
    }

}
