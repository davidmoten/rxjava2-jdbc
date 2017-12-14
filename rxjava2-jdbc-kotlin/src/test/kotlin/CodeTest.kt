package hello.tests

import hello.getHelloString
import kotlin.test.assertEquals
import org.junit.Test
import org.davidmoten.rx.jdbc.Database
import java.util.concurrent.TimeUnit

class HelloTest {
    @Test fun testAssert() : Unit {
        Database
          .test()
          .select("select name from person order by name")
          .getAs(String::class.java)
          .test()
          .awaitDone(20, TimeUnit.SECONDS)
          .assertValues("FRED", "JOSEPH", "MARMADUKE")
          .assertComplete()
    }
}
