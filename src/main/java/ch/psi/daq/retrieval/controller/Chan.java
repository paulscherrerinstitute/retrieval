package ch.psi.daq.retrieval.controller;

import java.nio.channels.SeekableByteChannel;

public class Chan extends OptCell<SeekableByteChannel> {

    public static Chan of(SeekableByteChannel k) {
        Chan ret = new Chan();
        ret.inner = k;
        return ret;
    }

}
