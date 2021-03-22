package ch.psi.daq.retrieval.finder;

import ch.psi.daq.retrieval.config.ChannelConfig;
import ch.psi.daq.retrieval.config.ChannelConfigEntry;
import ch.psi.daq.retrieval.reqctx.ReqCtx;
import ch.psi.daq.retrieval.utils.DateExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBuffer;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class Config {
    static final Logger LOGGER = LoggerFactory.getLogger(Config.class.getSimpleName());

    static ChannelConfig parseChannelConfig(ReqCtx reqCtx, LocatedChannel locatedChannel, DataBuffer buf) {
        ByteBuffer bbuf = buf.asByteBuffer(buf.readPosition(), buf.readableByteCount());
        short vser = bbuf.getShort();
        if (vser != 0) {
            LOGGER.error("{}  bad config ser {} {}", reqCtx, locatedChannel.name, vser);
            throw new RuntimeException("logic error");
        }
        int channelNameLength1 = bbuf.getInt();
        if (channelNameLength1 < 8 || channelNameLength1 > 256) {
            LOGGER.error("{}  channel name bad  {}  {}", reqCtx, locatedChannel.name, channelNameLength1);
            throw new RuntimeException("logic");
        }
        channelNameLength1 -= 8;
        StandardCharsets.UTF_8.decode(bbuf.slice().limit(channelNameLength1));
        bbuf.position(bbuf.position() + channelNameLength1);
        if (bbuf.getInt() != channelNameLength1 + 8) {
            LOGGER.error("{}  channel name bad  {}", reqCtx, channelNameLength1);
            throw new RuntimeException("logic");
        }
        ChannelConfig conf = new ChannelConfig();
        while (bbuf.remaining() > 0) {
            int p1 = bbuf.position();
            int len1 = bbuf.getInt();
            if (len1 < 32 || len1 > 1024) {
                LOGGER.error("{}  bad config entry len  {}", reqCtx, len1);
                throw new RuntimeException("logic");
            }
            if (bbuf.remaining() < len1 - Integer.BYTES) {
                LOGGER.error("bad config entry len  {}", len1);
                throw new RuntimeException("logic");
            }
            ChannelConfigEntry e = new ChannelConfigEntry();
            e.ts = bbuf.getLong();
            e.pulse = bbuf.getLong();
            e.ks = bbuf.getInt();
            e.bs = bbuf.getLong();
            e.sc = bbuf.getInt();
            int status = bbuf.getInt();
            byte bb = bbuf.get();
            int modulo = bbuf.getInt();
            int offset = bbuf.getInt();
            short precision = bbuf.getShort();
            int dtLen1 = bbuf.getInt();
            e.dtFlags = bbuf.get();
            e.dtType = bbuf.get();
            if ((e.dtFlags & 0x80) != 0) {
                e.isCompressed = true;
            }
            if ((e.dtFlags & 0x40) != 0) {
                e.isArray = true;
            }
            if ((e.dtFlags & 0x20) != 0) {
                e.isBigEndian = true;
            }
            if ((e.dtFlags & 0x10) != 0) {
                e.hasShape = true;
            }
            if (e.isCompressed) {
                byte method = bbuf.get();
            }
            if (e.hasShape) {
                e.dims = bbuf.get();
                if (e.dims < 0 || e.dims > 3) {
                    throw new RuntimeException("logic");
                }
                for (int i = 0; i < e.dims; i += 1) {
                    e.shape[i] = bbuf.getInt();
                }
            }
            LOGGER.debug("{}  found {}", reqCtx, e);
            conf.entries.add(e);
            bbuf.position(p1 + len1);
        }
        {
            for (int i1 = 1; i1 < conf.entries.size(); i1 += 1) {
                ChannelConfigEntry e1 = conf.entries.get(i1 - 1);
                ChannelConfigEntry e2 = conf.entries.get(i1);
                if (e1.ts >= e2.ts) {
                    LOGGER.error("{}  parseChannelConfig  unordered entries  ts1 {}  ts2 {}    {}", reqCtx, DateExt.toString(e1.ts), DateExt.toString(e2.ts), locatedChannel.name);
                    throw new RuntimeException("invalid");
                }
            }
        }
        return conf;
    }

}
