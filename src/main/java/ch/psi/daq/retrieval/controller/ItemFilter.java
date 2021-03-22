package ch.psi.daq.retrieval.controller;

import ch.psi.daq.retrieval.reqctx.ReqCtx;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.turbo.TurboFilter;
import ch.qos.logback.core.spi.FilterReply;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

public class ItemFilter extends TurboFilter {

    @Override
    public FilterReply decide(Marker marker, Logger logger, Level level, String format, Object[] objs, Throwable err) {
        if (marker != null) {
            if (marker.equals(ItemFilter.markBufTrace)) {
                return FilterReply.DENY;
            }
        }
        if (objs != null && objs.length >= 1) {
            Object o0 = objs[0];
            if (o0 instanceof ReqCtx) {
                ReqCtx ctx = (ReqCtx) o0;
                if (ctx.logLevel != null && level.isGreaterOrEqual(ctx.logLevel)) {
                    return FilterReply.ACCEPT;
                }
            }
        }
        if (marker != null) {
            if (marker.equals(QueryData.logMarkerRawLocalItem)) {
                return FilterReply.DENY;
            }
            if (marker.equals(QueryData.logMarkerWebClientResponseItem)) {
                return FilterReply.DENY;
            }
            if (marker.equals(QueryData.logMarkerAccept)) {
                return FilterReply.ACCEPT;
            }
        }
        return FilterReply.NEUTRAL;
    }

    public static final Marker markBufTrace = MarkerFactory.getMarker("markBufTrace");
    public static final Marker markPartsTrace = MarkerFactory.getMarker("markPartsTrace");

}
