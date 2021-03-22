package ch.psi.daq.retrieval.pod.api1;

public class Channel {

    public Channel(String backend, String name) {
        this.backend = backend;
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }

    public String backend;
    public String name;

}
