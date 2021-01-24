package ch.psi.daq.retrieval.status;

public class RequestStatusResult {

    RequestStatusResult(RequestStatus k) {
        ty = RequestStatusResultType.RequestStatus;
        requestStatus = k;
    }

    RequestStatusResult(RequestStatus.Error e) {
        ty = RequestStatusResultType.Error;
        error = e;
    }

    public boolean isStatus() {
        return ty == RequestStatusResultType.RequestStatus;
    }

    public boolean isError() {
        return ty == RequestStatusResultType.Error;
    }

    public RequestStatus status() {
        return requestStatus;
    }
    public RequestStatus.Error error() {
        return error;
    }

    enum RequestStatusResultType {
        RequestStatus,
        Error,
    }

    RequestStatusResultType ty;
    RequestStatus requestStatus;
    RequestStatus.Error error;

}
