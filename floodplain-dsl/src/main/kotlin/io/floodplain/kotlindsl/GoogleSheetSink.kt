package io.floodplain.kotlindsl

import com.dexels.kafka.streams.api.TopologyContext
import com.dexels.navajo.reactive.source.topology.SinkTransformer
import java.util.*


//    ->sink(connector='@staffsheets','@CUSTOMER',connect=true,columns='customer_id,last_name,first_name,email,total')
//<googlesheets.sink name="@staffsheets" connector.class="io.floodplain.sink.SheetSinkConnector" spreadsheetId="1COkG3-Y0phnHKvwNiFpYewKhT3weEC5CmzmKkXUpPA4" columns="last_name,first_name,email" />

fun PartialPipe.googleSheetsSink(config: GoogleSheetConfiguration) {
    val configMap: Map<String,String> = mapOf(Pair("connector.class","io.floodplain.sink.SheetSinkConnector"))
    val sink = SinkTransformer(config.topic, Optional.empty(), Optional.of(config.name), Optional.empty(), emptyMap() )
    addTransformer(Transformer(sink))
}

fun Pipe.googleSheetConfig(topic: String, name: String, spreadsheetId: String, columns: List<String>): GoogleSheetConfiguration {
    return GoogleSheetConfiguration(topic,name, spreadsheetId,columns)
}

class GoogleSheetConfiguration(val name: String, val topic: String, val spreadsheetId: String, val columns: List<String>): Config() {
    override fun materializeConnectorConfig(topologyContext: TopologyContext): Pair<String,Map<String, String>> {
        TODO("Not yet implemented")
    }

}