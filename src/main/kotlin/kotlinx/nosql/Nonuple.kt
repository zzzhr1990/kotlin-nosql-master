package kotlinx.nosql

class Nonuple<A1, A2, A3, A4, A5, A6, A7, A8, A9>(val a1: A1, val a2: A2, val a3: A3, val a4: A4, val a5: A5, val a6: A6, val a7: A7, val a8: A8, val a9: A9) {
    operator public fun component1(): A1 = a1
    operator public fun component2(): A2 = a2
    operator public fun component3(): A3 = a3
    operator public fun component4(): A4 = a4
    operator public fun component5(): A5 = a5
    operator public fun component6(): A6 = a6
    operator public fun component7(): A7 = a7
    operator public fun component8(): A8 = a8
    operator public fun component9(): A9 = a9
}