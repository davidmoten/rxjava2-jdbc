package hello
import org.davidmoten.rx.jdbc.Database

fun getHelloString() : String {
    Database.test()
    return "Hello, world!"
}

fun main(args : Array<String>) {
    println(getHelloString())
}

