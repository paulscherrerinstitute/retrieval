package ch.psi.daq.retrieval.finder;

import java.nio.file.Path;

public class StorageSettings {
    public Path baseDir;
    public String ksPrefix;

    public StorageSettings(Path baseDir, String ksPrefix) {
        this.baseDir = baseDir;
        this.ksPrefix = ksPrefix;
    }

}
