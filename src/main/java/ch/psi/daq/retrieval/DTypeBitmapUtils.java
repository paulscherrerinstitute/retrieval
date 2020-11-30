package ch.psi.daq.retrieval;

import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility class to make sense of the dtype  that describes a data event blob.
 * The dtype is a short that is split in two parts - lowest 8 bits used for type, upper 8 bits used as bitmask
 */
public class DTypeBitmapUtils {

    public enum Type {
        BOOL(0),
        BOOL8( 1),
        INT8(2),
        UINT8(3),
        INT16(4),
        UINT16(5),
        CHARACTER(6),
        INT32(7),
        UINT32(8),
        INT64(9),
        UINT64(10),
        FLOAT32(11),
        FLOAT64(12),
        STRING(13);

        private static final Map<Short, Type> lookup = new HashMap<>();
        static {
            for (Type d : Type.values()) {
                lookup.put(d.number, d);
            }
        }

        private final short number;
        Type(int number){
            this.number = (short) number;
        }

        public static Type lookup(short number){
            return(lookup.get(number));
        }
    }

    private static final short IS_COMPRESSED_BIT = 15;
    private static final short IS_ARRAY_BIT = 14;
    private static final short BYTE_ORDER_BIT = 13; // 1 = BigEndian - 0 LittleEndian
    private static final short HAS_SHAPE_BIT = 12;
    private static final short HAS_VERSION_BIT = 11; // currently not used ????

    // Use lowest 8 bits for real data type representation
    private static final short DATA_TYPE_MASK = 0b11111111;

    public static Type getType(short bitmask){
        return Type.lookup((short) (bitmask & DATA_TYPE_MASK));
    }

    public static boolean isCompressed(short bitmask){
        return isBitSet(bitmask, IS_COMPRESSED_BIT);
    }

    public static boolean isArray(short bitmask){
        return isBitSet(bitmask, IS_ARRAY_BIT);
    }

    public static ByteOrder getByteOrder(short bitmask){
        return isBitSet(bitmask, BYTE_ORDER_BIT) ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
    }

    public static boolean hasShape(short bitmask){
        return isBitSet(bitmask, HAS_SHAPE_BIT);
    }

    // Used to write the bitmask
    public static short calculateBitmask(Type type, ByteOrder byteOrder, boolean isArray, boolean isCompressed, boolean hasShape) {
        short bitmap = type.number;

        if (byteOrder.equals(ByteOrder.BIG_ENDIAN)) {
            bitmap |= (1 << BYTE_ORDER_BIT);
        }
        if (isArray) {
            bitmap |= (1 << IS_ARRAY_BIT);
        }
        if (isCompressed) {
            bitmap |= (1 << IS_COMPRESSED_BIT);
        }
        if (hasShape) {
            bitmap |= (1 << HAS_SHAPE_BIT);
        }

        return bitmap;
    }

    /**
     * Helper function to detect whether a certain bit is set in the given bitmask
     * @param bitmask   bitmask
     * @param position  position to check
     * @return true if bit at position is set
     */
    private static boolean isBitSet(short bitmask, int position) {
//        return position > 0 && position < Short.SIZE && (bitmask & (1 << position)) != 0;
        return (bitmask & (1 << position)) != 0; // for performance reasons we do not do all the checks above
    }
}
