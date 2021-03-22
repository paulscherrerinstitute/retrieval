package ch.psi.daq.retrieval.utils;

import org.reactivestreams.Subscription;

public class PubRepeatScr implements Subscription {

    public PubRepeatScr(PubRepeat<?> pub) {
        this.pub = pub;
    }

    @Override
    public synchronized void request(long n) {
        pub.request(n);
    }

    @Override
    public synchronized void cancel() {
        pub.cancel();
    }

    PubRepeat<?> pub;

}
