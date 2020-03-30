package io.floodplain.kotlindsl.htmlexample

import com.dexels.immutable.api.ImmutableMessage
import java.lang.ClassCastException
import java.lang.NullPointerException

class IMessage(input: Map<String,Any>) {

    private val content = input.toMutableMap()
//    val content = mapOf<String,Any>()
    private fun value(path: String): Any? {
        val (msg,name) = parsePath(path.split("/"))
        return msg.content.get(name)
    }

    fun string(path:String): String {
        return optionalString(path)?: throw NullPointerException("Can't obtain string from path: $path as it is absent")
    }
    fun integer(path:String): Int {
        return optionalInteger(path)?: throw NullPointerException("Can't obtain int from path: $path as it is absent")
    }

    fun optionalInteger(path:String): Int? {
        val raw = value(path) ?: return null
        if(raw !is Int) {
            throw ClassCastException("Path element $path should be an integer but it is a ${raw::class}")
        }
        return raw as Int
    }

    fun optionalString(path:String): String? {
        val raw = value(path) ?: return null
        if(raw !is String) {
            throw ClassCastException("Path element $path should be an string but it is a ${raw::class}")
        }
        return raw as String
    }

    private fun parsePath(path: List<String>): Pair<IMessage,String> {
        if(path.size>1) {
            val subelement: Any? = content[path[0]]
            subelement?: throw NullPointerException("Can't parse path list: $path, element ${path[0]} is missing")
            if(subelement !is IMessage) {
                throw ClassCastException("Path element ${path[0]} should be a message but it is a ${subelement::class}")
            }
            return subelement.parsePath(path.subList(1,path.size))
        }
        return Pair(this,path[0])

    }
    fun clear(path: String) {
        val (msg,name) = parsePath(path.split("/"))
        msg.content.remove(name)
    }

    fun set(path: String, value: Any) {
        val (msg,name) = parsePath(path.split("/"))
        msg.content.set(name,value)
    }
}

fun empty(): IMessage = IMessage(emptyMap())
