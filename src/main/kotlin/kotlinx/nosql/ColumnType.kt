package kotlinx.nosql

enum class ColumnType(val primitive: Boolean = false,
                      val iterable: Boolean = false,
                      val list: Boolean = false,
                      val set: Boolean = false,
                      val custom: Boolean = false,
                      val id: Boolean = false) {
    INTEGER(primitive = true),
    PRIMARY_ID(primitive = true, id = true),
    FOREIGN_ID(primitive = true, id = true),
    STRING(primitive = true),
    BOOLEAN(primitive = true),
    DATE(primitive = true),
    TIME(primitive = true),
    DATE_TIME(primitive = true),
    DOUBLE(primitive = true),
    FLOAT(primitive = true),
    LONG(primitive = true),
    SHORT(primitive = true),
    BYTE(primitive = true),
    INTEGER_SET(iterable = true, set = true),
    ID_SET(iterable = true, set = true, id = true),
    ID_LIST(iterable = true, list = true, id = true),
    STRING_SET(iterable = true, set = true),
    INTEGER_LIST(iterable = true, list = true),
    STRING_LIST(iterable = true, list = true),
    CUSTOM_CLASS(custom = true),
    CUSTOM_CLASS_LIST(iterable = true, custom = true, list = true),
    CUSTOM_CLASS_SET(iterable = true, custom = true, set = true)
}