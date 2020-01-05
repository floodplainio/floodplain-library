package io.floodplain;

import io.quarkus.test.junit.QuarkusTest;

import org.junit.Ignore;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.*;

@QuarkusTest
public class thingTest {

    @Test @Ignore
    public void testHelloEndpoint() {
        given()
          .when().get("/hello")
          .then()
             .statusCode(200)
             .body(containsString("aap.type"));
    }

}