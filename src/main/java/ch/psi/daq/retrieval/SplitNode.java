package ch.psi.daq.retrieval;

public class SplitNode {
    public int split;
    public String host;
    public int port;
    public SplitNode() {}
    public SplitNode(int split, String host, int port) {
        this.split = split;
        this.host = host;
        this.port = port;
    }
}
