package ch.psi.daq.retrieval.config;

import java.util.List;

public class Node {

    public String host;
    public int port;
    public List<Integer> splits;

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

}
