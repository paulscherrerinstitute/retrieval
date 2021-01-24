package ch.psi.daq.retrieval.eventmap.value;

import java.util.ArrayList;
import java.util.List;

public class MapJsonResult {
    private List<MapJsonItem> items = new ArrayList<>();
    private final boolean term;

    private enum Term {
        TERM,
    }

    public static MapJsonResult term() {
        return new MapJsonResult(Term.TERM);
    }

    public static MapJsonResult empty() {
        return new MapJsonResult();
    }

    private MapJsonResult(Term st) {
        term = true;
    }

    private MapJsonResult() {
        term = false;
    }

    public boolean isTerm() {
        return term;
    }

    public void release() {
        if (items != null) {
            for (MapJsonItem item : items) {
                item.release();
            }
            items.clear();
            items = null;
        }
    }

    public void addItem(MapJsonItem item) {
        items.add(item);
    }

    public List<MapJsonItem> getItems() {
        return items;
    }

}
