package ch.psi.daq.retrieval.config;

import java.util.List;

public class Node {

    public Node(String host, int port, List<Integer> splits) {
        this.host = host;
        this.port = port;
        this.splits = splits;
    }

    public Node(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public Node() {}

    @Override
    public String toString() {
        return String.format("%s:%d", host, port);
    }

    public String host;
    public int port;
    public List<Integer> splits;

}
