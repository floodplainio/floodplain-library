package io.floodplain

import javax.inject.Inject
import javax.sql.DataSource
import javax.ws.rs.GET
import javax.ws.rs.Path
import javax.ws.rs.Produces
import javax.ws.rs.core.MediaType
import io.floodplain.runtime.Main
import javax.enterprise.inject.Default

@Path("/hello")
class ExampleResource {

    @Inject
    lateinit var runtime: Main
    @Inject
    lateinit var source: DataSource
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    fun hello(): String {
        var r: com.dexels.navajo.reactive.api.ReactiveFinder;
        var rs = source.connection.prepareStatement("select * from city").executeQuery()
        var buffer = StringBuffer()
        while (rs.next()) {
            buffer.append(rs.getString(2))
            buffer.append("\n")
        }
        return buffer.toString()
    }
}