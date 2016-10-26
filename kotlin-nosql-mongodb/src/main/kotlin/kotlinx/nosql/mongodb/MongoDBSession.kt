package kotlinx.nosql.mongodb

import com.mongodb.BasicDBList
import com.mongodb.BasicDBObject
import com.mongodb.DBObject
import com.mongodb.client.MongoCursor
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model.IndexOptions
import kotlinx.nosql.*
import kotlinx.nosql.query.*
import kotlinx.nosql.util.*
import org.bson.Document
import org.bson.types.ObjectId
import org.joda.time.DateTime
import org.joda.time.LocalDate
import org.joda.time.LocalTime
import java.util.*
import java.util.logging.Logger
import java.util.regex.Pattern

class MongoDBSession(var mdb:MongoDB,var defaultDatabase:String = "test") : Session, DocumentSchemaOperations, TableSchemaOperations, IndexOperations {
    override fun switchDatabase(databaseName: String): Boolean {
        val gdb = mdb.client.getDatabase(databaseName)
        if(gdb == null){
            return false
        }
        db = gdb
        mdb.database = databaseName
        return true
    }


    val logger: Logger = Logger.getLogger(MongoDBSession::class.java.toString())
    override fun <T : Number> incr(schema: KeyValueSchema, column: AbstractColumn<out Any?, out AbstractSchema, T>, value: T): T {
        throw UnsupportedOperationException()
    }

    /*override fun <T : Number> incr(schema: AbstractSchema, column: AbstractColumn<out Any?, out AbstractSchema, T>, value: T, op: Query): T {
        throw UnsupportedOperationException()
    }*/
    val dbVersion: String
    val searchOperatorSupported: Boolean
    var db: MongoDatabase

    init {
        db = mdb.client.getDatabase(mdb.database)
        val results = db.runCommand(BasicDBObject("buildInfo", true))
        dbVersion = results!!["version"]!!.toString()
        logger.info("Init from version : " + dbVersion)
        val versions = dbVersion.split('.')
        searchOperatorSupported = versions[0].toInt() > 2 || (versions[0].toInt() == 2 && versions[1].toInt() >= 6)
    }


    override fun createIndex(schema: AbstractSchema, index: AbstractIndex) {
        val collection = db.getCollection(schema.schemaName)!!
        val dbObject = BasicDBObject()
        val i = (index as MongoDBIndex)
        for (column in i.ascending) {
            dbObject.append(column.name, 1)
        }
        for (column in i.descending) {
            dbObject.append(column.name, -1)
        }
        for (column in i.text) {
            dbObject.append(column.name, "text")
        }
        if (i.name.isNotEmpty())
            collection.createIndex(dbObject, IndexOptions().name(i.name))
        else
            collection.createIndex(dbObject)
    }

    override fun <T : AbstractSchema> T.create() {
        db.createCollection(this.schemaName)
    }

    override fun <T : AbstractSchema> T.drop() {
        val collection = db.getCollection(this.schemaName)!!
        collection.drop()
    }

    override fun <T : kotlinx.nosql.DocumentSchema<P, V>, P : Any, V : Any> T.insert(v: V): Id<P, T> {
        val collection = db.getCollection(this.schemaName)!!
        val doc = getDocument(v, this)
        if (discriminator != null) {
            var dominatorValue: Any? = null
            for (entry in kotlinx.nosql.DocumentSchema.discriminatorClasses.entries) {
                if (entry.value.java == v.javaClass) {
                    dominatorValue = entry.key.value
                }
            }
            doc.set(this.discriminator!!.column.name, dominatorValue!!)
        }
        collection.insertOne(doc)
        @Suppress("UNCHECKED_CAST")
        return Id<P, T>(doc.get("_id").toString() as P)
    }

    private fun getDocument(o: Any, schema: Any): Document {
        val doc = Document()
        val javaClass = o.javaClass
        val fields = getAllFields(javaClass)
        var sc: Class<out Any?>? = null
        var s: AbstractSchema? = null
        if (schema is kotlinx.nosql.DocumentSchema<*, *> && schema.discriminator != null) {
            for (entry in kotlinx.nosql.DocumentSchema.discriminatorClasses.entries) {
                if (entry.value.java == o.javaClass) {
                    sc = kotlinx.nosql.DocumentSchema.discriminatorSchemaClasses.get(entry.key)!!
                    s = kotlinx.nosql.DocumentSchema.discriminatorSchemas.get(entry.key)!!
                }
            }
        }
        val schemaClass: Class<out Any?> = if (schema is kotlinx.nosql.DocumentSchema<*, *> && schema.discriminator != null) sc!! else schema.javaClass
        val objectSchema: Any = if (schema is kotlinx.nosql.DocumentSchema<*, *> && schema.discriminator != null) s!! else schema

        @Suppress("UNCHECKED_CAST")
        val schemaFields = getAllFieldsMap(schemaClass as Class<in Any>, { f -> f.isColumn })
        for (field in fields) {
            val schemaField = schemaFields.get(field.getName()!!.toLowerCase())
            if (schemaField != null && schemaField.isColumn) {
                field.setAccessible(true)
                schemaField.setAccessible(true)
                val column = schemaField.asColumn(objectSchema)
                val value = field.get(o)
                if (value != null) {
                    if (column.columnType.primitive) {
                        doc.append(column.name, when (value) {
                            is DateTime, is LocalDate, is LocalTime -> value.toString()
                            is Id<*, *> -> ObjectId(value.value.toString())
                            else -> value
                        })
                    } else if (column.columnType.iterable) {
                        val list = BasicDBList()
                        @Suppress("UNCHECKED_CAST")
                        for (v in (value as Iterable<Any>)) {
                            list.add(if (column.columnType.custom) getDocument(v, column) else
                                (if (v is Id<*, *>) ObjectId(v.toString()) else v))
                        }
                        doc.append(column.name, list)
                    } else doc.append(column.name, getDocument(value, column))
                }
            }
        }
        return doc
    }

    override fun <T : kotlinx.nosql.DocumentSchema<P, C>, P : Any, C : Any> find(params: DocumentSchemaQueryParams<T, P, C>): Iterator<C> {
        if (params.query != null && !searchOperatorSupported && params.query!!.usesSearch())
            return params.schema.runCommandText(params.query!!)
        else
            return object : Iterator<C> {
                var cursor: MongoCursor<Document>? = null
                var pos = 0
                override fun next(): C {
                    if (cursor == null) {
                        val collection = db.getCollection(params.schema.schemaName)
                        val query = if (params.query != null) getQuery(params.query!!) else BasicDBObject()
                        logger.info(query.toJson())
                        val q = collection!!.find(query)!!
                        if (params.skip != null) {
                            q.skip(params.skip!!)
                        }
                        cursor = q.iterator()
                    }
                    val value = getObject(cursor!!.iterator().next(), params.schema) as C
                    pos++
                    if (!cursor!!.iterator().hasNext() || (params.take != null && pos == params.take!!)) {
                        cursor!!.close()
                        pos = -1

                    }
                    return value
                }

                override fun hasNext(): Boolean {
                    if (cursor == null) {
                        val collection = db.getCollection(params.schema.schemaName)
                        val query = if (params.query != null) getQuery(params.query!!) else BasicDBObject()
                        logger.info(query.toJson())
                        val q = collection!!.find(query)!!
                        if (params.skip != null) {
                            q.skip(params.skip!!)
                        }
                        cursor = q.iterator()
                    }
                    return pos != -1 && cursor!!.iterator().hasNext() && (params.take == null || pos < params.take!!)
                }
            }
    }

    private fun <T : kotlinx.nosql.DocumentSchema<P, C>, P : Any, C : Any> T.runCommandText(op: Query): Iterator<C> {
        val searchCmd = BasicDBObject()
        searchCmd.append("text", this.schemaName)
        // TODO: Only supports text(...) and other condition
        searchCmd.append("search", when (op) {
            is TextQuery -> op.search
            is AndQuery -> if (op.expr1 is TextQuery) (op.expr1 as TextQuery).search else throw UnsupportedOperationException()
            else -> throw UnsupportedOperationException()
        })
        val schema = this
        if (op is AndQuery) {
            searchCmd.append("filter", getQuery(op.expr2))
        }
        val result = db.runCommand(searchCmd)!!

        val objects = ArrayList<C>()
        for (doc in result.get("results") as BasicDBList) {
            objects.add(getObject((doc as Document).get("obj") as Document, schema))
        }

        return objects.iterator()
    }

    override fun <T : TableSchema<P>, P : Any, V : Any> find(params: TableSchemaProjectionQueryParams<T, P, V>): Iterator<V> {
        return object : Iterator<V> {
            var cursor: MongoCursor<Document>? = null
            var pos = 0
            override fun next(): V {
                if (cursor == null) {
                    val collection = db.getCollection(params.table.schemaName)
                    val fields = BasicDBObject()
                    params.projection.forEach {
                        fields.append(it.fullName, "1")
                    }
                    val query = if (params.query != null) getQuery(params.query!!) else BasicDBObject()
                    logger.info("=" + query.toJson())
                    val q = collection!!.find(query)!!
                    if (params.skip != null) {
                        q.skip(params.skip!!)
                    }
                    cursor = q.iterator()
                }
                val doc = cursor!!.next()
                val values = ArrayList<Any?>()
                params.projection.forEach {
                    values.add(getColumnObject(doc, it))
                }
                @Suppress("UNCHECKED_CAST")
                val value = when (values.size) {
                    1 -> values[0] as V
                    2 -> Pair(values[0], values[1]) as V
                    3 -> Triple(values[0], values[1], values[2]) as V
                    4 -> Quadruple(values[0], values[1], values[2], values[3]) as V
                    5 -> Quintuple(values[0], values[1], values[2], values[3], values[4]) as V
                    6 -> Sextuple(values[0], values[1], values[2], values[3], values[4], values[5]) as V
                    7 -> Septuple(values[0], values[1], values[2], values[3], values[4], values[5], values[6]) as V
                    8 -> Octuple(values[0], values[1], values[2], values[3], values[4], values[5], values[6], values[7]) as V
                    9 -> Nonuple(values[0], values[1], values[2], values[3], values[4], values[5], values[6], values[7], values[8]) as V
                    10 -> Decuple(values[0], values[1], values[2], values[3], values[4], values[5], values[6], values[7], values[8], values[9]) as V
                    else -> throw UnsupportedOperationException()
                }
                pos++
                if (!cursor!!.hasNext() || (params.take != null && pos == params.take!!)) {
                    cursor!!.close()
                    pos = -1
                }
                return value

            }

            override fun hasNext(): Boolean {
                if (cursor == null) {
                    val collection = db.getCollection(params.table.schemaName)
                    val fields = BasicDBObject()
                    params.projection.forEach {
                        fields.append(it.fullName, "1")
                    }
                    val query = if (params.query != null) getQuery(params.query!!) else BasicDBObject()
                    logger.info("=" + query.toJson())
                    val q = collection!!.find(query)!!
                    if (params.skip != null) {
                        q.skip(params.skip!!)
                    }
                    cursor = q.iterator()
                }
                return pos != -1 && cursor!!.hasNext() && (params.take == null || pos < params.take!!)
            }
        }
    }

    private fun Query.usesSearch(): Boolean {
        return when (this) {
            is TextQuery -> true
            is OrQuery -> this.expr1.usesSearch() || this.expr2.usesSearch()
            is AndQuery -> this.expr1.usesSearch() || this.expr2.usesSearch()
            else -> false
        }
    }

    protected fun getQuery(op: Query, removePrefix: String = ""): BasicDBObject {
        val query = BasicDBObject()
        when (op) {
            is EqualQuery -> {
                if (op.expr1 is AbstractColumn<*, *, *>) {
                    if (op.expr2 is LiteralExpression) {
                        if ((op.expr1 as AbstractColumn<*, *, *>).columnType.primitive) {
                            if ((op.expr1 as AbstractColumn<*, *, *>).columnType.id) {
                                query.append((op.expr1 as AbstractColumn<*, *, *>).fullName, ObjectId((op.expr2 as LiteralExpression).value.toString()))
                            } else {
                                var columnName = (op.expr1 as AbstractColumn<*, *, *>).fullName
                                if (removePrefix.isNotEmpty() && columnName.startsWith(removePrefix)) {
                                    columnName = columnName.substring(removePrefix.length + 1)
                                }
                                query.append(columnName, (op.expr2 as LiteralExpression).value)
                            }
                        } else {
                            throw UnsupportedOperationException()
                        }
                    } else if (op.expr2 is AbstractColumn<*, *, *>) {
                        query.append("\$where", "this.${(op.expr1 as AbstractColumn<*, *, *>).fullName} == this.${(op.expr2 as AbstractColumn<*, *, *>).fullName}")
                    } else {
                        throw UnsupportedOperationException()
                    }
                } else {
                    throw UnsupportedOperationException()
                }
            }
            is MatchesQuery -> {
                if (op.expr1 is AbstractColumn<*, *, *>) {
                    if (op.expr2 is LiteralExpression) {
                        if ((op.expr2 as LiteralExpression).value is Pattern) {
                            query.append((op.expr1 as AbstractColumn<*, *, *>).fullName, BasicDBObject().append("\$regex", (op.expr2 as LiteralExpression).value))
                        } else {
                            throw UnsupportedOperationException()
                        }
                    } else {
                        throw UnsupportedOperationException()
                    }
                } else {
                    throw UnsupportedOperationException()
                }
            }
            is NotEqualQuery -> {
                if (op.expr1 is AbstractColumn<*, *, *>) {
                    if (op.expr2 is LiteralExpression) {
                        if ((op.expr2 as LiteralExpression).value is String || (op.expr2 as LiteralExpression).value is Int) {
                            if ((op.expr1 as AbstractColumn<*, *, *>).columnType.id) {
                                query.append((op.expr1 as AbstractColumn<*, *, *>).fullName, BasicDBObject().append("\$ne", ObjectId((op.expr2 as LiteralExpression).value.toString())))
                            } else {
                                query.append((op.expr1 as AbstractColumn<*, *, *>).fullName, BasicDBObject().append("\$ne", (op.expr2 as LiteralExpression).value))
                            }
                        } else {
                            throw UnsupportedOperationException()
                        }
                    } else if (op.expr2 is AbstractColumn<*, *, *>) {
                        query.append("\$where", "this.${(op.expr1 as AbstractColumn<*, *, *>).fullName} != this.${(op.expr2 as AbstractColumn<*, *, *>).fullName}")
                    } else {
                        throw UnsupportedOperationException()
                    }
                } else {
                    throw UnsupportedOperationException()
                }
            }
            is GreaterQuery -> {
                if (op.expr1 is AbstractColumn<*, *, *>) {
                    if (op.expr2 is LiteralExpression) {
                        if ((op.expr2 as LiteralExpression).value is String || (op.expr2 as LiteralExpression).value is Number) {
                            query.append((op.expr1 as AbstractColumn<*, *, *>).fullName, BasicDBObject().append("\$gt", (op.expr2 as LiteralExpression).value))
                        } else {
                            throw UnsupportedOperationException()
                        }
                    } else if (op.expr2 is AbstractColumn<*, *, *>) {
                        query.append("\$where", "this.${(op.expr1 as AbstractColumn<*, *, *>).fullName} > this.${(op.expr2 as AbstractColumn<*, *, *>).fullName}")
                    } else {
                        throw UnsupportedOperationException()
                    }
                } else {
                    throw UnsupportedOperationException()
                }
            }
            is LessQuery -> {
                if (op.expr1 is AbstractColumn<*, *, *>) {
                    if (op.expr2 is LiteralExpression) {
                        if ((op.expr2 as LiteralExpression).value is String || (op.expr2 as LiteralExpression).value is Number) {
                            query.append((op.expr1 as AbstractColumn<*, *, *>).fullName, BasicDBObject().append("\$lt", (op.expr2 as LiteralExpression).value))
                        } else {
                            throw UnsupportedOperationException()
                        }
                    } else if (op.expr2 is AbstractColumn<*, *, *>) {
                        query.append("\$where", "this.${(op.expr1 as AbstractColumn<*, *, *>).fullName} < this.${(op.expr2 as AbstractColumn<*, *, *>).fullName}")
                    } else {
                        throw UnsupportedOperationException()
                    }
                } else {
                    throw UnsupportedOperationException()
                }
            }
            is GreaterEqualQuery -> {
                if (op.expr1 is AbstractColumn<*, *, *>) {
                    if (op.expr2 is LiteralExpression) {
                        if ((op.expr2 as LiteralExpression).value is String || (op.expr2 as LiteralExpression).value is Number) {
                            query.append((op.expr1 as AbstractColumn<*, *, *>).fullName, BasicDBObject().append("\$gte", (op.expr2 as LiteralExpression).value))
                        } else {
                            throw UnsupportedOperationException()
                        }
                    } else if (op.expr2 is AbstractColumn<*, *, *>) {
                        query.append("\$where", "this.${(op.expr1 as AbstractColumn<*, *, *>).fullName} >= this.${(op.expr2 as AbstractColumn<*, *, *>).fullName}")
                    } else {
                        throw UnsupportedOperationException()
                    }
                } else {
                    throw UnsupportedOperationException()
                }
            }
            is LessEqualQuery -> {
                if (op.expr1 is AbstractColumn<*, *, *>) {
                    if (op.expr2 is LiteralExpression) {
                        if ((op.expr2 as LiteralExpression).value is String || (op.expr2 as LiteralExpression).value is Number) {
                            query.append((op.expr1 as AbstractColumn<*, *, *>).fullName, BasicDBObject().append("\$lte", (op.expr2 as LiteralExpression).value))
                        } else {
                            throw UnsupportedOperationException()
                        }
                    } else if (op.expr2 is AbstractColumn<*, *, *>) {
                        query.append("\$where", "this.${(op.expr1 as AbstractColumn<*, *, *>).fullName} <= this.${(op.expr2 as AbstractColumn<*, *, *>).fullName}")
                    } else {
                        throw UnsupportedOperationException()
                    }
                } else {
                    throw UnsupportedOperationException()
                }
            }
            is MemberOfQuery -> {
                if (op.expr1 is AbstractColumn<*, *, *>) {
                    if (op.expr2 is LiteralExpression) {
                        if ((op.expr2 as LiteralExpression).value is List<*> || (op.expr2 as LiteralExpression).value is Array<*>) {
                            query.append((op.expr1 as AbstractColumn<*, *, *>).fullName, BasicDBObject().append("\$in", (op.expr2 as LiteralExpression).value))
                        } else {
                            throw UnsupportedOperationException()
                        }
                    } else {
                        throw UnsupportedOperationException()
                    }
                } else {
                    throw UnsupportedOperationException()
                }
            }
            is NotMemberOfQuery -> {
                if (op.expr1 is AbstractColumn<*, *, *>) {
                    if (op.expr2 is LiteralExpression) {
                        if ((op.expr2 as LiteralExpression).value is List<*> || (op.expr2 as LiteralExpression).value is Array<*>) {
                            query.append((op.expr1 as AbstractColumn<*, *, *>).fullName, BasicDBObject().append("\$nin", (op.expr2 as LiteralExpression).value))
                        } else {
                            throw UnsupportedOperationException()
                        }
                    } else {
                        throw UnsupportedOperationException()
                    }
                } else {
                    throw UnsupportedOperationException()
                }
            }
        // TODO TODO TODO eq expression and eq expression
            is AndQuery -> {

                /*
                val query1 = getQuery(op.expr1)
                val query2 = getQuery(op.expr2)
                for (entry in query1.entries) {
                    logger.info("exp1" + query1.toJson())
                    query.append(entry.key, entry.value)
                }
                for (entry in query2.entries) {
                    logger.info("exp2" + query1.toJson())
                    query.append(entry.key, entry.value)
                }
                return query
                */

                query.append("\$and", Arrays.asList(getQuery(op.expr1), getQuery(op.expr2)))

            }
            is OrQuery -> {
                query.append("\$or", Arrays.asList(getQuery(op.expr1), getQuery(op.expr2)))
            }
            is TextQuery -> {
                query.append("\$text", BasicDBObject().append("\$search", op.search))
            }
            is NoQuery -> {
                // Do nothing
            }
            else -> {
                throw UnsupportedOperationException()
            }
        }
        return query
    }

    private fun <T : kotlinx.nosql.DocumentSchema<P, V>, P : Any, V : Any /* not sure */> getObject(doc: Document, schema: T): V {
        var s: AbstractSchema? = null
        val valueInstance: Any = if (schema is kotlinx.nosql.DocumentSchema<*, *> && schema.discriminator != null) {
            var instance: Any? = null
            val discriminatorValue = doc.get(schema.discriminator!!.column.name)
            for (discriminator in kotlinx.nosql.DocumentSchema.tableDiscriminators.get(schema.schemaName)!!) {
                if (discriminator.value == discriminatorValue) {
                    instance = newInstance(kotlinx.nosql.DocumentSchema.discriminatorClasses.get(discriminator)!!.java)
                    s = kotlinx.nosql.DocumentSchema.discriminatorSchemas.get(discriminator)!!
                    break
                }
            }
            instance!!
        } else {
            s = schema
            newInstance(schema.valueClass.java)
        }
        val schemaClass = s!!.javaClass
        @Suppress("UNCHECKED_CAST")
        val schemaFields = getAllFields(schemaClass as Class<in Any?>)
        @Suppress("UNCHECKED_CAST")
        val valueFields = getAllFieldsMap(valueInstance.javaClass as Class<in Any?>)
        for (schemaField in schemaFields) {
            if (AbstractColumn::class.java.isAssignableFrom(schemaField.getType()!!)) {
                val valueField = valueFields.get(if (schemaField.getName()!! == "pk") "id" else schemaField.getName()!!.toLowerCase())
                if (valueField != null) {
                    schemaField.isAccessible = true
                    valueField.isAccessible = true
                    val column = schemaField.asColumn(s)
                    val value = doc.get(column.name)
                    @Suppress("UNCHECKED_CAST")
                    val columnValue: Any? = if (value == null) {
                        null
                    } else if (column.columnType.id && !column.columnType.iterable)
                        Id<P, T>(value.toString() as P)
                    else if (column.columnType.primitive) {
                        when (column.columnType) {
                            ColumnType.DATE -> LocalDate(value.toString())
                            ColumnType.TIME -> LocalTime(value.toString())
                            ColumnType.DATE_TIME -> DateTime(value.toString())
                            else -> doc.get(column.name)
                        }
                    } else if (column.columnType.list && !column.columnType.custom) {
                        (doc.get(column.name) as BasicDBList).toList()
                    } else if (column.columnType.set && !column.columnType.custom && !column.columnType.id) {
                        (doc.get(column.name) as BasicDBList).toSet()
                    } else if (column.columnType.id && column.columnType.set) {
                        val list = doc.get(column.name) as BasicDBList
                        list.map { Id<String, TableSchema<String>>(it.toString()) }.toSet()
                    } else if (column.columnType.custom && column.columnType.set) {
                        val list = doc.get(column.name) as BasicDBList
                        list.map { getObject(it as Document, column as ListColumn<*, out AbstractSchema>) }.toSet()
                    } else if (column.columnType.custom && column.columnType.list) {
                        val list = doc.get(column.name) as BasicDBList
                        list.map { getObject(it as Document, column as ListColumn<*, out AbstractSchema>) }.toList()
                    } else {
                        getObject(doc.get(column.name) as Document, column as Column<Any, T>)
                    }
                    if (columnValue != null || column is AbstractNullableColumn) {
                        valueField.set(valueInstance, columnValue)
                    } else {
                        throw NullPointerException()
                    }
                }
            }
        }
        @Suppress("UNCHECKED_CAST")
        return valueInstance as V
    }

    private fun getObject(doc: Document, column: AbstractColumn<*, *, *>): Any? {
        val valueInstance = newInstance(column.valueClass.java)
        val schemaClass = column.javaClass
        val columnFields = schemaClass.declaredFields!!
        @Suppress("UNCHECKED_CAST")
        val valueFields = getAllFieldsMap(valueInstance.javaClass as Class<in Any?>)
        for (columnField in columnFields) {
            if (columnField.isColumn) {
                val valueField = valueFields.get(columnField.getName()!!.toLowerCase())
                if (valueField != null) {
                    columnField.isAccessible = true
                    valueField.isAccessible = true
                    @Suppress("NAME_SHADOWING")
                    val column = columnField.asColumn(column)
                    val columnValue: Any? = if (column.columnType.id && !column.columnType.iterable) Id<String, TableSchema<String>>(doc.get(column.name).toString())
                    else if (column.columnType.primitive) doc.get(column.name)
                    else if (column.columnType.list && !column.columnType.custom) (doc.get(column.name) as BasicDBList).toList()
                    else if (column.columnType.set && !column.columnType.custom && !column.columnType.id) (doc.get(column.name) as BasicDBList).toSet()
                    else if (column.columnType.custom && column.columnType.list) {
                        val list = doc.get(column.name) as BasicDBList
                        list.map { getObject(it as Document, column as ListColumn<*, out AbstractSchema>) }.toList()
                    } else if (column.columnType.id && column.columnType.set) {
                        val list = doc.get(column.name) as BasicDBList
                        list.map { Id<String, TableSchema<String>>(it.toString()) }.toSet()
                    } else if (column.columnType.custom && column.columnType.set) {
                        val list = doc.get(column.name) as BasicDBList
                        list.map { getObject(it as Document, column as ListColumn<*, out AbstractSchema>) }.toSet()
                    } else {
                        getObject(doc.get(column.name) as Document, column as Column<*, out AbstractSchema>)
                    }
                    if (columnValue != null || column is AbstractNullableColumn) {
                        valueField.set(valueInstance, columnValue)
                    } else {
                        throw NullPointerException()
                    }
                }
            }
        }
        return valueInstance
    }

    override fun <T : AbstractSchema> insert(columns: Array<Pair<AbstractColumn<out Any?, T, out Any>, Any?>>) {
        throw UnsupportedOperationException()
    }

    override fun <T : AbstractSchema> delete(table: T, op: Query): Int {
        val collection = db.getCollection(table.schemaName)!!
        val query = getQuery(op)
        return collection.deleteMany(query)!!.deletedCount.toInt()
    }

    override fun update(schema: AbstractSchema, columnValues: Array<Pair<AbstractColumn<*, *, *>, *>>, op: Query): Int {
        val collection = db.getCollection(schema.schemaName)!!
        val statement = BasicDBObject()
        val doc = BasicDBObject().append("\$set", statement)
        for ((column, value) in columnValues) {
            statement.append(column.fullName, getDBValue(value, column))
        }
        return collection.updateMany(getQuery(op), doc)!!.modifiedCount.toInt()
    }

    override fun <T> addAll(schema: AbstractSchema, column: AbstractColumn<Collection<T>, *, *>, values: Collection<T>, op: Query): Int {
        val collection = db.getCollection(schema.schemaName)!!
        val statement = BasicDBObject()
        val doc = BasicDBObject().append("\$pushAll", statement)
        statement.append(column.fullName, getDBValue(values, column))
        return collection.updateMany(getQuery(op), doc)!!.modifiedCount.toInt()
    }

    override fun <T> removeAll(schema: AbstractSchema, column: AbstractColumn<Collection<T>, *, *>, values: Collection<T>, op: Query): Int {
        val collection = db.getCollection(schema.schemaName)!!
        val statement = BasicDBObject()
        val doc = BasicDBObject().append("\$pullAll", statement)
        statement.append(column.fullName, getDBValue(values, column))
        return collection.updateMany(getQuery(op), doc)!!.modifiedCount.toInt()
    }

    override fun <T> removeAll(schema: AbstractSchema, column: AbstractColumn<Collection<T>, *, *>, removeOp: Query, op: Query): Int {
        val collection = db.getCollection(schema.schemaName)!!
        val statement = BasicDBObject()
        val doc = BasicDBObject().append("\$pull", statement)
        statement.append(column.fullName, getQuery(removeOp, column.fullName))
        return collection.updateMany(getQuery(op), doc)!!.modifiedCount.toInt()
    }

    private fun getDBValue(value: Any?, column: AbstractColumn<*, *, *>, withinIterable: Boolean = false): Any? {
        return if (!column.columnType.custom && (!column.columnType.iterable || withinIterable)) {
            when (value) {
                is DateTime, is LocalDate, is LocalTime -> value.toString()
                is Id<*, *> -> ObjectId(value.value.toString())
                else -> value
            }
        } else if (column.columnType.custom && !column.columnType.iterable)
            if (value != null) getDocument(value, column) else null
        else if (column.columnType.list && column.columnType.custom)
            (value as List<*>).map { getDocument(it!!, column) }
        else if (column.columnType.set && column.columnType.custom)
            (value as Set<*>).map { getDocument(it!!, column) }.toSet()
        else if (column.columnType.list && !column.columnType.custom)
            (value as List<*>).map { getDBValue(it!!, column, true) }
        else if (column.columnType.set && !column.columnType.custom)
            (value as Set<*>).map { getDBValue(it!!, column, true) }.toSet()
        else throw UnsupportedOperationException()
    }

    private fun getColumnObject(doc: Document, column: AbstractColumn<*, *, *>): Any? {
        val columnObject = parse(doc, column.fullName.split(".").toTypedArray())
        return if (column.columnType.id && !column.columnType.iterable) {
            Id<String, TableSchema<String>>(columnObject.toString())
        } else if (column.columnType.primitive && !column.columnType.iterable) when (column.columnType) {
            ColumnType.DATE -> LocalDate.parse(columnObject.toString())
            ColumnType.TIME -> LocalTime.parse(columnObject.toString())
            ColumnType.DATE_TIME -> DateTime.parse(columnObject.toString())
            else -> columnObject
        } else if (column.columnType == ColumnType.STRING_SET) {
            (columnObject as BasicDBList).toSet()
        } else if (column.columnType == ColumnType.STRING_LIST) {
            (columnObject as BasicDBList).toList()
        } else if (column.columnType.id && column.columnType.set) {
            (columnObject as BasicDBList).map { Id<String, TableSchema<String>>(it.toString()) }.toSet()
        } else if (column.columnType.id && column.columnType.list) {
            (columnObject as BasicDBList).map { Id<String, TableSchema<String>>(it.toString()) }
        } else if (column.columnType.custom && column.columnType.list) {
            @Suppress("UNCHECKED_CAST")
            (columnObject as BasicDBList).map { getDocument(it as DBObject, column as ListColumn<Any, out AbstractSchema>) }
        } else if (column.columnType.custom && column.columnType.set) {
            @Suppress("UNCHECKED_CAST")
            (columnObject as BasicDBList).map { getDocument(it as DBObject, column as ListColumn<Any, out AbstractSchema>) }.toSet()
        } else if (column.columnType.custom) {
            getDocument(columnObject as Document, column)
        } else {
            UnsupportedOperationException()
        }
    }

    private fun parse(doc: Document, path: Array<String>, position: Int = 0): Any? {
        val value = doc.get(path[position])
        if (position < path.size - 1) {
            return parse(value as Document, path, position + 1)
        } else {
            return value
        }
    }

    override fun <T : kotlinx.nosql.DocumentSchema<P, C>, P : Any, C : Any> T.find(query: T.() -> Query): DocumentSchemaQueryWrapper<T, P, C> {
        val params = DocumentSchemaQueryParams<T, P, C>(this, query())
        return DocumentSchemaQueryWrapper(params)
    }
}
