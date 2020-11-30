package ch.psi.daq.retrieval.finder;

import java.nio.file.Path;

public class BaseDir implements Comparable<BaseDir> {
    public Path baseDir;
    public String baseKeyspaceName;

    public BaseDir(Path baseDir, String baseKeyspaceName) {
        this.baseDir = baseDir;
        this.baseKeyspaceName = baseKeyspaceName;
    }

    @Override
    public int compareTo(BaseDir x) {
        int n = baseDir.compareTo(x.baseDir);
        if (n != 0) {
            return n;
        }
        else {
            return baseKeyspaceName.compareTo(x.baseKeyspaceName);
        }
    }

    @Override
    public String toString() {
        return String.format("BaseDir { baseDir: %s, baseKeyspaceName: %s }", baseDir.toString(), baseKeyspaceName);
    }

}
