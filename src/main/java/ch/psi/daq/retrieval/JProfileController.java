package ch.psi.daq.retrieval;

public class JProfileController {

    static void findProfiler() {
        ClassLoader classLoader = Main.class.getClassLoader();
        try {
            Class<?> cl1 = classLoader.loadClass("jprof4");
            cl1.getMethod("setup").invoke(null);
        }
        catch (ClassNotFoundException e) {
        }
        catch (Throwable e) {
        }
    }

}
