package hello.tests

import hello.getHelloString
import kotlin.test.assertEquals
import org.junit.Test
import org.davidmoten.rx.jdbc.Database

class HelloTest {
    @Test fun testAssert() : Unit {
        Database.test().select("select name from person").getAs(javaClass<String>()).forEach({x -> println(x)})
        assertEquals("Hello, world!", getHelloString())
    }
}
