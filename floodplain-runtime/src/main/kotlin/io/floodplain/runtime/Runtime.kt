package io.floodplain.runtime
import com.xenomachina.argparser.ArgParser
import com.xenomachina.argparser.default
import io.floodplain.kotlindsl.LocalContext
import io.floodplain.kotlindsl.Stream
import java.lang.RuntimeException
import java.net.URL

internal class LocalArgs(parser: ArgParser) {
    val verbose by parser.flagging(
        "-v", "--verbose",
        help = "enable verbose mode")

    val force by parser.flagging(
        "-f", "--force",
        help = "force redeploy of connect modules. Not for local executions.")

    val kafka by parser.storing(
        "-k", "--kafka",
        help = """Point to a kafka cluster e.g. localhost:9092 if none is given, floodplain will run in kafkaless mode. 
|                 If a kafka cluster is supplied, you probably need a --connect as well""".trimMargin())
        .default<String?>(null)

    val connect by parser.storing(
        "-c", "--connect",
        help = """point to a connect instance e.g. http://localhost:8083
        |Make sure that this connect instance contains all required connector code
        """.trimMargin())
        .default<String?>(null)

    // val source by parser.positional(
    //     "SOURCE",
    //     help = "source filename")
    //
    // val destination by parser.positional(
    //     "DEST",
    //     help = "destination filename")
}

fun run(stream: Stream, arguments: Array<String>, localContext: (LocalContext.() -> Unit)?) {
    ArgParser(arguments).parseInto(::LocalArgs).run {
        if (kafka != null) {
            if (connect == null) {
                throw RuntimeException("When supplying kafka, supply connect too")
            }
            stream.renderAndSchedule(URL(connect), kafka!!, force)
        } else {
            stream.renderAndExecute {
                if (localContext != null) {
                    localContext(this)
                }
            }
        }
        // TODO: move widgets
    }
}
