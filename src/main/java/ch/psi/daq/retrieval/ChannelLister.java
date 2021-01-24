package ch.psi.daq.retrieval;

import ch.psi.daq.retrieval.config.ConfigurationDatabase;
import ch.psi.daq.retrieval.config.ConfigurationRetrieval;
import ch.psi.daq.retrieval.eventmap.value.OutputBuffer;
import ch.psi.daq.retrieval.pod.api1.Order;
import ch.qos.logback.classic.Logger;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import reactor.core.publisher.SynchronousSink;

import java.io.IOException;
import java.sql.*;
import java.util.List;

public class ChannelLister {
    static final Logger LOGGER = (Logger) LoggerFactory.getLogger(ChannelLister.class.getSimpleName());
    ConfigurationRetrieval conf;
    DataBufferFactory bufFac;
    ResultSet res;
    OutputBuffer outbuf;
    JsonFactory jfac;
    JsonGenerator jgen;
    //JsonNodeFactory jnf = JsonNodeFactory.instance;
    boolean hasError;
    Order order;
    String nameRegex;
    String sourceRegex;
    String descriptionRegex;
    boolean configOut;
    ObjectMapper om = new ObjectMapper();
    public static ChannelLister create(ConfigurationRetrieval conf, DataBufferFactory bufFac, Order order, String nameRegex, String sourceRegex, String descriptionRegex, boolean configOut) {
        //LOGGER.info("ChannelLister create");
        ChannelLister ret = new ChannelLister();
        ret.conf = conf;
        ret.order = order;
        ret.configOut = configOut;
        if (nameRegex == null) {
            nameRegex = "";
        }
        if (sourceRegex == null) {
            sourceRegex = "";
        }
        if (descriptionRegex == null) {
            descriptionRegex = "";
        }
        ret.nameRegex = nameRegex;
        ret.sourceRegex = sourceRegex;
        ret.descriptionRegex = descriptionRegex;
        ret.bufFac = bufFac;
        ret.outbuf = new OutputBuffer(bufFac, 8 * 1024);
        ret.jfac = new JsonFactory();
        try {
            ret.startJson();
        }
        catch (IOException e) {
            LOGGER.error("{}", e.toString());
            ret.hasError = true;
        }
        try {
            ret.startQuery();
        }
        catch (SQLException e) {
            LOGGER.error("{}", e.toString());
            ret.hasError = true;
        }
        return ret;
    }
    void startJson() throws IOException {
        jgen = jfac.createGenerator(outbuf);
        jgen.writeStartArray();
        jgen.writeStartObject();
        jgen.writeStringField("backend", conf.backend);
        jgen.writeFieldName("channels");
        jgen.writeStartArray();
    }
    void startQuery() throws SQLException {
        ConfigurationDatabase c = conf.database;
        String dbUrl = String.format("jdbc:postgresql://%s:%d/%s", c.host, c.port, c.database);
        try (Connection conn = DriverManager.getConnection(dbUrl, c.username, c.password)) {
            startQueryWithConnection(conn);
        }
    }
    void startQueryWithConnection(Connection conn) throws SQLException {
        PreparedStatement st;
        String ord = "asc";
        if (order == Order.DESC) {
            ord = "desc";
        }
        String selectColumns = "channel_id, channel_name, source_name, dtype, shape, unit, description";
        String sql = "select " + selectColumns + " from searchext(?, ?, ?, ?)";
        st = conn.prepareStatement(sql);
        st.setString(1, nameRegex);
        st.setString(2, sourceRegex);
        st.setString(3, descriptionRegex);
        st.setString(4, ord);
        res = st.executeQuery();
    }
    public static ChannelLister generate(ChannelLister self, SynchronousSink<List<DataBuffer>> sink) {
        if (self.hasError) {
            sink.error(new RuntimeException("error already during create"));
        }
        else {
            try {
                self.gen(sink);
            }
            catch (IOException | SQLException e) {
                sink.error(e);
            }
        }
        return self;
    }
    void gen(SynchronousSink<List<DataBuffer>> sink) throws IOException, SQLException {
        int i1 = 0;
        while (res.next() && outbuf.totalPending() < 32 * 1024) {
            if (configOut) {
                jgen.writeStartObject();
                jgen.writeStringField("backend", conf.backend);
                jgen.writeStringField("name", res.getString(2));
                jgen.writeStringField("source", res.getString(3));
                jgen.writeStringField("type", res.getString(4));
                String sh = res.getString(5);
                if (!res.wasNull() && !sh.equals("null")) {
                    List<Integer> a = om.readValue(sh, new TypeReference<>() {});
                    if (a != null) {
                        jgen.writeArrayFieldStart("shape");
                        for (int i : a) {
                            jgen.writeNumber(i);
                        }
                        jgen.writeEndArray();
                    }
                }
                jgen.writeStringField("unit", res.getString(6));
                jgen.writeStringField("description", res.getString(7));
                jgen.writeEndObject();
            }
            else {
                String name = res.getString(2);
                jgen.writeString(name);
            }
            i1 += 1;
        }
        if (i1 == 0) {
            jgen.writeEndArray();
            jgen.writeEndObject();
            jgen.writeEndArray();
            jgen.close();
            sink.next(outbuf.getPending());
            sink.complete();
        }
        else {
            sink.next(outbuf.getPending());
        }
    }
    public static void release(ChannelLister self) {
        LOGGER.info("ChannelLister release");
    }
}
