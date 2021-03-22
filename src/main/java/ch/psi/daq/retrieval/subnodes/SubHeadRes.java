package ch.psi.daq.retrieval.subnodes;

import ch.psi.daq.retrieval.bytes.BufCont;
import ch.psi.daq.retrieval.config.ChannelConfigEntry;
import ch.psi.daq.retrieval.merger.Releasable;

import java.util.List;

class SubHeadRes implements Releasable {

    @Override
    public void releaseFinite() {
        if (bufs != null) {
            for (BufCont bc : bufs) {
                bc.close();
            }
            bufs.clear();
        }
    }

    ChannelConfigEntry channelConfigEntry;
    List<BufCont> bufs;

}
