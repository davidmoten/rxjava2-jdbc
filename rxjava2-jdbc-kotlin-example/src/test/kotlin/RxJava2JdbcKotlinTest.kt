package rx.jdbc.kotlin

import kotlin.test.assertEquals
import org.junit.Test
import org.davidmoten.rx.jdbc.Database
import org.davidmoten.rx.jdbc.annotations.Query
import org.davidmoten.rx.jdbc.annotations.Column
import java.util.concurrent.TimeUnit

class RxJava2JdbcTest {
    @Test 
    fun testSelect() : Unit {
        Database
          .test()
          .select("select name from person order by name")
          .getAs(String::class.java)
          .test()
          .awaitDone(20, TimeUnit.SECONDS)
          .assertValues("FRED", "JOSEPH", "MARMADUKE")
          .assertComplete()
    }

    @Test
    fun testAutomap() : Unit {
        Database
          .test()
          .select(Person::class.java)
          .get()
          .map({x -> x.nm()})
          .test()
          .awaitDone(20, TimeUnit.SECONDS)
          .assertValues("FRED", "JOSEPH", "MARMADUKE")
          .assertComplete()
    }
}

@Query("select name from person order by name")
interface Person {
    @Column("name")
    fun nm() : String
}

