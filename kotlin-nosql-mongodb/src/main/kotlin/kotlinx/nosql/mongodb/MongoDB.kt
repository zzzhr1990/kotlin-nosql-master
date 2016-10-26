package kotlinx.nosql.mongodb

import kotlinx.nosql.Database
import kotlinx.nosql.Session
import com.mongodb.MongoClient
import kotlinx.nosql.AbstractColumn
import kotlinx.nosql.util.*
import java.util.concurrent.ConcurrentHashMap
import com.mongodb.ServerAddress
import com.mongodb.MongoClientOptions
import com.mongodb.MongoClientURI
import kotlinx.nosql.SchemaGenerationAction
import kotlinx.nosql.Create
import kotlinx.nosql.CreateDrop
import kotlinx.nosql.Validate
import kotlinx.nosql.Update
import kotlinx.nosql.AbstractSchema
import com.mongodb.MongoCredential

/*
fun MongoDB(uri: MongoClientURI, schemas: Array<out AbstractSchema>, initialization: SchemaGenerationAction<MongoDBSession> = Validate()): MongoDB {
    val seeds: Array<ServerAddress> = uri.getHosts()!!.map { host ->
        if (host.indexOf(':') > 0) {
            val tokens = host.split(':')
            ServerAddress(tokens[0], tokens[1].toInt())
        } else
            ServerAddress(host)
    }.toTypedArray()
    val database: String = if (uri.database != null) uri.getDatabase()!! else "test"
    val options: MongoClientOptions = uri.getOptions()!!
    val credentials = if (uri.getUsername() != null)
      arrayOf(MongoCredential.createMongoCRCredential(uri.getUsername(), database, uri.getPassword())!!)
    else arrayOf()
  return MongoDB(seeds, database, credentials, options, schemas, initialization)
}

// TODO: Allow use more than one database

class MongoDB(val seeds: Array<ServerAddress> , val database: String = "test",
              val credentials: Array<MongoCredential> = arrayOf(), val options: MongoClientOptions = MongoClientOptions.Builder().build()!!,
              schemas: Array<out AbstractSchema>, action: SchemaGenerationAction<MongoDBSession> = Validate()) : Database<MongoDBSession>(schemas, action) {
    val db = MongoClient(seeds.toList(), credentials.toList(), options).getDatabase(database)!!
    var session = MongoDBSession(db)

    init {
        initialize()
    }

    // TODO: Use session pool
    override fun <R> withSession(statement: MongoDBSession.() -> R): R {
        Session.threadLocale.set(session)
        val r = session.statement()
        Session.threadLocale.set(null)
        return r
    }



}

*/

class MongoDB(val client:MongoClient,schemas: Array<out AbstractSchema>, var database: String = "test",action: SchemaGenerationAction<MongoDBSession> = Validate()): Database<MongoDBSession>(schemas, action){


    override fun <R> withSession(statement: MongoDBSession.() -> R): R {
        return mongoSession(statement)
    }


    //var session = MongoDBSession(client.getDatabase(database))

    init {
        SessionManager._manager = ThreadLocalSessionManager(this)
        initialize()
    }

   // fun getDatabase() = client.getDatabase(database)

}

fun<R> mongoSession(statement: MongoDBSession.() -> R):R {
    return mongoSession(statement,null)
}
fun<R> mongoSession(statement: MongoDBSession.() -> R,switchDB:String?):R {
    //We Need To Find DataBase Sessions
    // Get MongoDB...
    val outer = SessionManager.currentOrNull()
    var oriDB:String? = null
    try {
        return if (outer != null) {
            if(switchDB != null){
                oriDB = outer.defaultDatabase
                outer.switchDatabase(switchDB)
            }
            val r = outer.statement()
            if(oriDB != null){
                outer.switchDatabase(oriDB)
            }
            r
        }
        else {
            val transaction = SessionManager.currentThreadManager.get().newSession()
            if(switchDB != null){
                oriDB = transaction.defaultDatabase
                transaction.switchDatabase(switchDB)
            }
            val  r = transaction.statement()
            if(oriDB != null){
                transaction.switchDatabase(oriDB)
            }
            r
        }
    }finally {
        SessionManager
    }

}


class ThreadLocalSessionManager(private val db:MongoDB):SessionManager{
    override fun clean() {
        threadLocal.remove()
        Session.threadLocale.remove()
    }

    override fun newSession(): MongoDBSession = MongoDBSession(db).apply {
        threadLocal.set(this)
        Session.threadLocale.set(this)
    }

    override fun currentOrNull(): MongoDBSession? = threadLocal.get()

    val threadLocal = ThreadLocal<MongoDBSession>()
    
}

interface SessionManager {

    fun newSession() : MongoDBSession

    fun clean()

    fun currentOrNull(): MongoDBSession?

    companion object {

        @Volatile lateinit internal var _manager: SessionManager

        var currentThreadManager = object : ThreadLocal<SessionManager>() {
            override fun initialValue(): SessionManager = _manager
        }

        fun currentOrNew() = currentOrNull() ?: currentThreadManager.get().newSession()

        fun currentOrNull() = currentThreadManager.get().currentOrNull()

        fun current() = currentOrNull() ?: error("No transaction in context.")

        fun clean() = currentThreadManager.get().clean()
    }
}