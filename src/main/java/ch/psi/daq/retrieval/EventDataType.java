package ch.psi.daq.retrieval;

public enum EventDataType {
    BOOL,
    BOOL8,
    INT8,
    UINT8,
    INT16,
    UINT16,
    CHARACTER,
    INT32,
    UINT32,
    INT64,
    UINT64,
    FLOAT32,
    FLOAT64,
    STRING;

    public boolean isInt() {
        return compareTo(INT8) >= 0 && compareTo(UINT64) <= 0 && this != CHARACTER;
    }

    public boolean isUnsignedInt() {
        return this == UINT8 || this == UINT16 || this == UINT32 || this == UINT64;
    }

    public boolean isSignedInt() {
        return this == INT8 || this == INT16 || this == INT32 || this == INT64;
    }

    public boolean isFloat() {
        return this == FLOAT32 || this == FLOAT64;
    }

    public int sizeOf() {
        switch (this) {
            case UINT8:   return 1;
            case UINT16:  return 2;
            case UINT32:  return 4;
            case UINT64:  return 8;
            case INT8:    return 1;
            case INT16:   return 2;
            case INT32:   return 4;
            case INT64:   return 8;
            case FLOAT32: return 4;
            case FLOAT64: return 8;
            default: {
                throw new RuntimeException("todo");
            }
        }
    }

    static public EventDataType[] vals;
    static {
        vals = EventDataType.values();
    }

}
