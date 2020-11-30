package ch.psi.daq.retrieval.eventmap.value;

import java.util.ArrayList;
import java.util.List;

public class MapJsonResult {
    public boolean term;
    public List<MapJsonItem> items = new ArrayList<>();

    public void release() {
        if (items != null) {
            for (MapJsonItem item : items) {
                item.release();
            }
            items.clear();
            items = null;
        }
    }

}
