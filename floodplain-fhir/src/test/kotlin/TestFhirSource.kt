import ca.uhn.fhir.context.FhirContext
import ca.uhn.fhir.parser.IParser
import io.floodplain.fhir.genericResource
import io.floodplain.fhir.parseFhirToMsg
import io.floodplain.kotlindsl.message.IMessage
import io.floodplain.kotlindsl.message.empty
import org.hl7.fhir.r4.model.Patient
import org.junit.Assert
import org.junit.Test
import java.io.ByteArrayInputStream

class TestFhirSource {

    private fun patientToMessage(patient: Patient): IMessage {
        val message = empty()
        message["name"] = patient.name[0].family
        message["birthdate"] = patient.birthDate
        return message
    }
    @Test
    fun testParse() {
        val parser: IParser = FhirContext.forR4().newJsonParser()
        val data = this::class.java.classLoader.getResource("patient-example.json").readBytes()
        val msg = parseFhirToMsg(parser,data,::patientToMessage)
        Assert.assertEquals("Chalmers",msg["name"])
    }

    @Test
    fun testParseGenericFhirResource() {
        val parser: IParser = FhirContext.forR4().newJsonParser()
        val data = this::class.java.classLoader.getResource("patientexample.json").readBytes()
        val resource = parser.parseResource(ByteArrayInputStream(data))
        val message = genericResource(resource)
        println("message: $message")
    }
}