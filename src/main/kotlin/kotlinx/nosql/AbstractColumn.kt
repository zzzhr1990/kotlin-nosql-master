package kotlinx.nosql

import java.util.ArrayList
import java.util.regex.Pattern
import kotlinx.nosql.query.*
import kotlin.collections.listOf
import kotlin.collections.setOf
import kotlin.reflect.KClass

open class AbstractColumn<C, T : AbstractSchema, S: Any>(val name: String, val valueClass: KClass<S>, val columnType: ColumnType) : ColumnQueryWrapper<C>(), Expression<C> {
    internal var _schema: AbstractSchema? = null

    @Suppress("UNCHECKED_CAST")
    val schema: T
        get() {
            return _schema!! as T
        }

    fun matches(other: Pattern): Query {
        return MatchesQuery(this, LiteralExpression(other))
    }

    override fun toString(): String {
        return name
    }

    operator fun <C2> plus(c: AbstractColumn<C2, T, *>): ColumnPair<T, C, C2> {
        return ColumnPair(this, c) as ColumnPair<T, C, C2>
    }

}

/*
fun<C, T: AbstractSchema> AbstractColumn<C, T, *>.get(): C {
    throw UnsupportedOperationException()
}
*/

fun <C, T: AbstractSchema> AbstractColumn<C, T, *>.update(value: C): Int {
    val wrapper = TableSchemaProjectionQueryWrapper.get()
    return Session.current<Session>().update(wrapper.params.table, arrayOf(wrapper.params.projection.get(0) to value),
            wrapper.params.query!!)
}

fun <C, S: Collection<C>> AbstractColumn<S, *, *>.addAll(values: S): Int {
    val wrapper = TableSchemaProjectionQueryWrapper.get()
    @Suppress("UNCHECKED_CAST")
    return Session.current<Session>().addAll(wrapper.params.table,
            wrapper.params.projection.get(0)
                    as AbstractColumn<Collection<C>, out AbstractSchema, out Any>, values,
            wrapper.params.query!!)
}

fun <C, S: Collection<C>> AbstractColumn<S, *, *>.add(value: C): Int {
    val wrapper = TableSchemaProjectionQueryWrapper.get()
    val values: Collection<C> = if (wrapper.params.projection.get(0).columnType.list) listOf(value) else setOf(value)
    @Suppress("UNCHECKED_CAST")
    return Session.current<Session>().addAll(wrapper.params.table,
                    wrapper.params.projection.get(0)
                            as AbstractColumn<Collection<C>, out AbstractSchema, out Any>, values,
                    wrapper.params.query!!)
}

fun <C, S: Collection<C>> AbstractColumn<S, *, *>.removeAll(values: S): Int {
    val wrapper = TableSchemaProjectionQueryWrapper.get()
    @Suppress("UNCHECKED_CAST")
    return Session.current<Session>().removeAll(wrapper.params.table,
            wrapper.params.projection.get(0)
                    as AbstractColumn<Collection<C>, out AbstractSchema, out Any>, values,
            wrapper.params.query!!)
}

fun <C> AbstractColumn<out Collection<C>, *, *>.remove(value: C): Int {
    val wrapper = TableSchemaProjectionQueryWrapper.get()
    val values: Collection<C> = if (wrapper.params.projection.get(0).columnType.list) listOf(value) else setOf(value)
    @Suppress("UNCHECKED_CAST")
    return Session.current<Session>().removeAll(wrapper.params.table,
            wrapper.params.projection.get(0)
                    as AbstractColumn<Collection<C>, out AbstractSchema, out Any>, values,
            wrapper.params.query!!)
}

fun <C: AbstractColumn<out Collection<*>, *, *>> C.remove(removeOp: C.() -> Query): Int {
    val wrapper = TableSchemaProjectionQueryWrapper.get()
        val removeOpValue = with (wrapper.params.projection.get(0)) {
            removeOp()
        }
    @Suppress("UNCHECKED_CAST")
    return Session.current<Session>().removeAll(wrapper.params.table,
            wrapper.params.projection.get(0)
                    as AbstractColumn<Collection<C>, out AbstractSchema, out Any>, removeOpValue,
            wrapper.params.query!!)
}

fun <T : AbstractSchema, C: Any> AbstractColumn<C?, T, C>.isNull(): Query {
    return IsNullQuery(this)
}

fun <T : AbstractSchema, C: Any> AbstractColumn<C?, T, C>.notNull(): Query {
    return IsNotNullQuery(this)
}

fun <T : AbstractSchema, C> AbstractColumn<out C?, T, *>.equal(other: C): Query {
    return EqualQuery(this, LiteralExpression(other))
}

fun <T : AbstractSchema, C> AbstractColumn<out C?, T, *>.notEqual(other: C): Query {
    return NotEqualQuery(this, LiteralExpression(other))
}

fun <T : AbstractSchema, C> AbstractColumn<out C?, T, *>.memberOf(other: Iterable<C>): Query {
    return MemberOfQuery(this, LiteralExpression(other))
}

fun <T : AbstractSchema, C> AbstractColumn<out C?, T, *>.memberOf(other: Array<C>): Query {
    return MemberOfQuery(this, LiteralExpression(other))
}

// TODO TODO TODO: Expression should be typed
fun <T : AbstractSchema, C> AbstractColumn<out C?, T, *>.memberOf(other: Expression<out Iterable<C>>): Query {
    return MemberOfQuery(this, LiteralExpression(other))
}

fun <T : AbstractSchema, C> AbstractColumn<out C?, T, *>.notMemberOf(other: Iterable<C>): Query {
    return NotMemberOfQuery(this, LiteralExpression(other))
}

fun <T : AbstractSchema, C> AbstractColumn<out C?, T, *>.notMemberOf(other: Array<C>): Query {
    return NotMemberOfQuery(this, LiteralExpression(other))
}

fun <T : AbstractSchema, C> AbstractColumn<out C?, T, *>.notMemberOf(other: Expression<out Iterable<C>>): Query {
    return NotMemberOfQuery(this, LiteralExpression(other))
}

fun <T : AbstractSchema, C: Any> AbstractColumn<out C?, T, C>.equal(other: Expression<out C?>): Query {
    return EqualQuery(this, other)
}

fun <T : AbstractSchema, C: Any> AbstractColumn<out C?, T, C>.notEqual(other: Expression<out C?>): Query {
    return NotEqualQuery(this, other)
}

fun <T : AbstractSchema> AbstractColumn<out Int?, T, Int>.gt(other: Expression<out Int?>): Query {
    return GreaterQuery(this, other)
}

fun <T : AbstractSchema> AbstractColumn<out Int?, T, Int>.gt(other: Int): Query {
    return GreaterQuery(this, LiteralExpression(other))
}

fun <T : AbstractSchema> AbstractColumn<out Long?, T, Long>.gt(other: Long): Query {
    return GreaterQuery(this, LiteralExpression(other))
}

fun <T : AbstractSchema> AbstractColumn<out Int?, T, Int>.ge(other: Expression<out Int?>): Query {
    return GreaterEqualQuery(this, other)
}

fun <T : AbstractSchema> AbstractColumn<out Int?, T, Int>.ge(other: Int): Query {
    return GreaterEqualQuery(this, LiteralExpression(other))
}

fun <T : AbstractSchema> AbstractColumn<out Int?, T, Int>.le(other: Expression<out Int?>): Query {
    return LessEqualQuery(this, other)
}

fun <T : AbstractSchema> AbstractColumn<out Int?, T, Int>.le(other: Int): Query {
    return LessEqualQuery(this, LiteralExpression(other))
}

fun <T : AbstractSchema> AbstractColumn<out Int?, T, Int>.lt(other: Expression<out Int?>): Query {
    return LessQuery(this, other)
}

fun <T : AbstractSchema> AbstractColumn<out Int?, T, Int>.lt(other: Int): Query {
    return LessQuery(this, LiteralExpression(other))
}

fun <T : AbstractSchema> AbstractColumn<out Long?, T, Long>.lt(other: Long): Query {
    return LessQuery(this, LiteralExpression(other))
}