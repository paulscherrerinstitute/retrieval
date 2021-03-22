package ch.psi.daq.retrieval.finder;

import ch.psi.daq.retrieval.config.ChannelConfigEntry;

public class MatchingChannelConfigRange {

    MatchingChannelConfigRange(ChannelConfigEntry entry, long untilMs) {
        this.entry = entry;
        this.untilMs = untilMs;
    }

    ChannelConfigEntry entry;
    long untilMs;

}
