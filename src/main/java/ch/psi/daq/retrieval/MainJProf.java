package ch.psi.daq.retrieval;

import ch.psi.daq.retrieval.controller.ServiceInfo;

import java.net.URL;
import java.net.URLClassLoader;

public class MainJProf {

    static void optionalLocateProfiler() {
        try {
            URL[] urls = { new URL("file:/opt/retrieval/jprof4.jar") };
            URLClassLoader cll = new URLClassLoader(urls);
            ServiceInfo.profClass = cll.loadClass("jprof4.Info");
        }
        catch (Throwable e) {
        }
    }

}
