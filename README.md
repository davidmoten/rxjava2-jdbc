# rxjava2-jdbc

With the release of RxJava 2 now is a good time for a rewrite of [rxjava-jdbc](https://github.com/davidmoten/rxjava-jdbc). 

See [wiki](https://github.com/davidmoten/rxjava2-jdbc/wiki)

Status: *in development*

JDBC is so much simpler with *rxjava2-jdbc*:

Getting started
------------------
Use this maven dependency:

```xml
<dependency>
  <groupId>com.github.davidmoten.rxjava2-jdbc</groupId>
  <artifactId>rxjava2-jdbc</artifactId>
  <version>VERSION_HERE</version>
</dependency>
```
If you want to use the built-in test database then add the Apache Derby dependency (otherwise you'll need the jdbc dependency for the database you want to connect to):

```xml
<dependency>
  <groupId>org.apache.derby</groupId>
  <artifactId>derby</artifactId>
  <version>10.13.1.1</version>
</dependency>
```

Database
-------------
To start things off you need a `Database` instance. Given the jdbc url of your database you can create a `Database` object like this:

```java
Database db = Database.from(url, maxPoolSize);
```

If you want to have a play with the built-in test database then do this:

```java
Database db = Database.test(maxPoolSize);
```

The test database has a couple of tables `Person` and `Address` with three rows in `Person` and two rows in `Address`:

<img src="src/docs/tables.png?raw=true"/>

A query example
---------------
Let's use the `Database` instance to perform a select query on the `Person` table and write the names to the console:

```java
Database db = Database.test();
db.select("select name from person")
  .getAs(String.class)
  .forEach(System.out::println);
```

Output is
```
FRED
JOSEPH
MARMADUKE
```

That example is very brief but for these simple tests it's preferable to use a different subscribe method to `forEach` because if we had for instance asked for a column that did not exist then an exception stack trace would be written to stderr but no exception would be thrown. This example will throw a `SQLRuntimeException` because the column `nam` does not exist:

```java
Database db = Database.test();
db.select("select nam from person")
  .getAs(String.class)
  .doOnNext(System.out::println)
  .blockingSubscribe();
```

Tuple support
-----------------
When you specify more types in the `getAs` method they are matched to the columns in the returned result set from the query and combined into a `Tuple` instance. Here's an example that returns `Tuple2`:

```java
Database db = Database.test();
db.select("select name, score from person")
  .getAs(String.class, Integer.class)
  .doOnNext(System.out::println)
  .blockingSubscribe();
```
Output
```
Tuple2 [value1=FRED, value2=21]
Tuple2 [value1=JOSEPH, value2=34]
Tuple2 [value1=MARMADUKE, value2=25]
```
Tuples are defined from `Tuple2` to `Tuple7` and for above that to `TupleN`.

Automap
--------------
To map the result set values to an interface, first declare an interface:

```java
interface Person {
  @Column
  String name();

  @Column
  int score();
}
```

In the query use the `autoMap` method and let's use some of the built-in testing methods of RxJava2 to confirm we got what we expected:

```java
Database db = Database.test();
db.select("select name, score from person order by name")
  .autoMap(Person.class)
  .doOnNext(System.out::println)
  .firstOrError()
  .map(Person::score) 
  .test()
  .assertValue(21) 
  .assertComplete();
```

If your interface method name does not exactly match the column name (underscores and case are ignored) then you can add more detail to the `Column` annotation:

```java
interface Person {
  @Column("name")
  String fullName();

  @Column("score")
  int examScore();
}
```

Non-blocking connection pools
-------------------------------
A new exciting feature of *rxjava2-jdbc* is the availability of non-blocking connection pools. 

In normal non-reactive database programming a couple of different threads (started by servlet calls for instance) will *race* for the next available connection from a pool of database connections. If no unused connection remains in the pool then the standard non-reactive approach is to **block the thread** until a connection becomes available. This is a resource issue as each blocked thread holds onto ~0.5MB of stack and may incur context switch and memory-access delays (adds latency to thread processing). 

*rxjava-jdbc2* uses non-blocking JDBC connection pools by default (but is configurable to use whatever you want). 

The simplest way of creating a `Database` instance with a non-blocking connection pool is:

```java
Database db = Database.from(url, maxPoolSize);
```

If you want to play with the in-memory built-in test database (requires Apache Derby dependency) then:

```java
Database db = Database.test(maxPoolSize);

```
If you want more control over the behaviour of the non-blocking connection pool:

```java
NonBlockingConnectionPool pool = Pools
    .nonBlocking()
    .url(url)
    .healthy(c -> c.prepareStatement("select 1").execute())
    .maxPoolSize(3)
    .build();
Database db = Database.from(pool);
```

Note that the health check sql varies from database to database. Here are some examples:

* Oracle - `select 1 from dual`
* Sql Server - `select 1`
* H2 - `select 1`
* Derby - `select 1 from sysibm.sysdummy1`

### Demonstration

Lets create a database with a non-blocking connection pool of size 1 only and demonstrate what happens when two queries run concurrently. We use the in-built test database for this one 
so you can copy and paste this code to your ide and it will run (in a main method or unit test say):

```java
// create database with non-blocking connection pool 
// of size 1
Database db = Database.test(1); 

//start a slow query
db.select("select score from person where name=?") 
  .parameters("FRED") 
  .getAs(Integer.class) 
  // slow things down by sleeping
  .doOnNext(x -> Thread.sleep(1000)) 
  // run in background thread
  .subscribeOn(Schedulers.io()) 
  .subscribe();

//ensure that query starts
Thread.sleep(100);

//query again while first query running
db.select("select score from person where name=?") 
  .parameters("FRED") 
  .getAs(Integer.class) //
  .doOnNext(x -> System.out.println("emitted on " + Thread.currentThread().getName())) //
  .subscribe();

System.out.println("second query submitted");

//wait for stuff to happen asynchronously
Thread.sleep(5000);
```

The output of this is 

```
second query submitted
emitted on RxCachedThreadScheduler-1
```

What has happened is that 
* the second query registers itself as something that will run as soon as a connection is released (by the first query). 
* no blocking occurs and we immediately see the first line of output
* the second query runs after the first
* in fact we see that the second query runs on the same Thread as the first query as a direct consequence of non-blocking design  




