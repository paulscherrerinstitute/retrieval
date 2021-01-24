package ch.psi.daq.retrieval.controller;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

class TokenSinker {
    BlockingQueue<Integer> queue;
    AtomicLong n1 = new AtomicLong();

    TokenSinker(int n) {
        queue = new LinkedBlockingQueue<>(3 * n + 3);
        for (int i1 = 0; i1 < n; i1 += 1) {
            putBack(i1);
        }
    }

    void putBack(int token) {
        queue.add((((int) n1.getAndAdd(1)) * 10000) + (token % 10000));
    }
}
