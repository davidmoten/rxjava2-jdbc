# rxjava2-jdbc
[![Travis CI](https://travis-ci.org/davidmoten/rxjava2-jdbc.svg)](https://travis-ci.org/davidmoten/rxjava2-jdbc)<br/>
[![codecov](https://codecov.io/gh/davidmoten/rxjava2-jdbc/branch/master/graph/badge.svg)](https://codecov.io/gh/davidmoten/rxjava2-jdbc)
<!--[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.github.davidmoten/rxjava2-jdbc/badge.svg?style=flat)](https://maven-badges.herokuapp.com/maven-central/com.github.davidmoten/rxjava2-jdbc)<br/>-->

With the release of RxJava 2 now is a good time for a rewrite of [rxjava-jdbc](https://github.com/davidmoten/rxjava-jdbc). 

See [wiki](https://github.com/davidmoten/rxjava2-jdbc/wiki)

Status: *in development*

JDBC is so much simpler with *rxjava2-jdbc*.

How to build
-------------
Use maven:
```bash
mvn clean install
```

Getting started
------------------
Use this maven dependency:

```xml
<dependency>
  <groupId>com.github.davidmoten.rxjava2-jdbc</groupId>
  <artifactId>rxjava2-jdbc</artifactId>
  <version>0.1-SNAPSHOT</version>
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

### Support for playing with rxjava2-jdbc!

If you want to have a play with a built-in test database then do this:

```java
Database db = Database.test(maxPoolSize);
```

The test database has a couple of tables `Person` and `Address` with three rows in `Person` and two rows in `Address`:

<img src="src/docs/tables.png?raw=true"/>

Each time you call `Database.test(maxPoolSize)` you will have a fresh new database to play with that is loaded with data as described above.

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
  .blockingForEach(System.out::println);
```

Tuple support
-----------------
When you specify more types in the `getAs` method they are matched to the columns in the returned result set from the query and combined into a `Tuple` instance. Here's an example that returns `Tuple2`:

```java
Database db = Database.test();
db.select("select name, score from person")
  .getAs(String.class, Integer.class)
  .blockingForEach(System.out::println);
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

You can also refer to the 1-based position of the column in the result set instead of its name:
```java
interface Person {
  @Index(1)
  String fullName();

  @Index(2)
  int examScore();
}
```

In fact, you can mix use of named columns and indexed columns in automapped interfaces.

If you don't configure things correctly these exceptions may be emitted and include extra information in the error message about the affected automap interface:

* `AnnotationsNotFoundException`
* `ColumnIndexOutOfRangeException`
* `ColumnNotFoundException`
* `ClassCastException`

Auto mappings
------------------
The automatic mappings below of objects are used in the ```autoMap()``` method and for typed ```getAs()``` calls.
* ```java.sql.Date```,```java.sql.Time```,```java.sql.Timestamp``` <==> ```java.util.Date```
* ```java.sql.Date```,```java.sql.Time```,```java.sql.Timestamp```  ==> ```java.lang.Long```
* ```java.sql.Blob``` <==> ```java.io.InputStream```, ```byte[]```
* ```java.sql.Clob``` <==> ```java.io.Reader```, ```String```
* ```java.math.BigInteger``` ==> ```Long```, ```Integer```, ```Decimal```, ```Float```, ```Short```, ```java.math.BigDecimal```
* ```java.math.BigDecimal``` ==> ```Long```, ```Integer```, ```Decimal```, ```Float```, ```Short```, ```java.math.BigInteger```

Note that automappings do not occur to primitives so use ```Long``` instead of ```long```.

Parameters
----------------
Parameters are passed to individual queries but can also be used as a streaming source to prompt the query to be run many times.

Parameters can be named or anonymous. Named parameters are not supported natively by the JDBC specification but *rxjava2-jdbc* does support them.

This is sql with a named parameter:

```sql
select name from person where name=:name
```

This is sql with an anonymous parameter:

```sql
select name from person where name=?
```

### Explicit anonymous parameters

In the example below the query is first run with `name='FRED'` and then `name=JOSEPH`. Each query returns one result which is printed to the console.

```java
Database.test()
  .select("select score from person where name=?") 
  .parameters("FRED", "JOSEPH")
  .getAs(Integer.class)
  .blockingForEach(System.out::println);
```
Output is:
```
21
34
```

### Flowable anonymous parameters

You can specify a stream as the source of parameters:

```java
Database.test()
  .select("select score from person where name=?") 
  .parameterStream(Flowable.just("FRED","JOSEPH").repeat())
  .getAs(Integer.class)
  .take(3)
  .blockingForEach(System.out::println);
```

Output is:
```
21
34
21
```

## Mixing explicit and Flowable parameters

```java
Database.test()
  .select("select score from person where name=?") 
  .parameterStream(Flowable.just("FRED","JOSEPH"))
  .parameters("FRED", "JOSEPH")
  .getAs(Integer.class)
  .blockingForEach(System.out::println);
```
Output is:
```
21
34
21
34
```
## Multiple parameters per query

If there is more than one parameter per query:

```java
Database.test()
  .select("select score from person where name=? and score=?") 
  .parameterStream(Flowable.just("FRED", 21, "JOSEPH", 34).repeat())
  .getAs(Integer.class)
  .take(3)
  .blockingForEach(System.out::println);
```
or you can group the parameters into lists (each list corresponds to one query) yourself:

```java
Database.test()
  .select("select score from person where name=? and score=?") 
  .parameterListStream(Flowable.just(Arrays.asList("FRED", 21), Arrays.asList("JOSEPH", 34)).repeat())
  .getAs(Integer.class)
  .take(3)
  .blockingForEach(System.out::println);
```

## Running a query many times that has no parameters
If the query has no parameters you can use the parameters to drive the number of query calls (the parameter values themselves are ignored):

```java
Database.test()
  .select("select count(*) from person") 
  .parameters("a", "b", "c")
  .getAs(Integer.class)
  .blockingForEach(System.out::println);
```

Output:
```
3
3
3
```

Non-blocking connection pools
-------------------------------
A new exciting feature of *rxjava2-jdbc* is the availability of non-blocking connection pools. 

In normal non-reactive database programming a couple of different threads (started by servlet calls for instance) will *race* for the next available connection from a pool of database connections. If no unused connection remains in the pool then the standard non-reactive approach is to **block the thread** until a connection becomes available. 

Blocking a thread is a resource issue as each blocked thread holds onto ~0.5MB of stack and may incur context switch and memory-access delays (adds latency to thread processing) when being switched to. For example 100 blocked threads hold onto ~50MB of memory (outside of java heap).

*rxjava-jdbc2* uses non-blocking JDBC connection pools by default (but is configurable to use whatever you want). What happens in practice is that for each query a subscription is made to a `PublishSubject` controlled by the `NonBlockingConnectionPool` object that emits connections when available to its subscribers (first in best dressed). So the definition of the processing of that query is stored on a queue to be started when a connection is available. TODO estimate the size on the heap of a sample *rxjava2-jdbc*-based Flowable stream.

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
     an unused connection will be closed after thirty minutes
    .maxIdleTime(30, TimeUnit.MINUTES)
     connections are checked for healthiness on checkout if the connection 
     has been idle for at least `idleTimeBeforeHealthCheckMs`
    .healthy(c -> c.prepareStatement("select 1").execute())
    .idleTimeBeforeHealthCheckMs(1, TimeUnit.MINUTES)
    .returnToPoolDelayAfterHealthCheckFailure(1, TimeUnit.SECONDS) 
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
 create database with non-blocking connection pool 
 of size 1
Database db = Database.test(1); 

start a slow query
db.select("select score from person where name=?") 
  .parameters("FRED") 
  .getAs(Integer.class) 
   slow things down by sleeping
  .doOnNext(x -> Thread.sleep(1000)) 
   run in background thread
  .subscribeOn(Schedulers.io()) 
  .subscribe();

ensure that query starts
Thread.sleep(100);

query again while first query running
db.select("select score from person where name=?") 
  .parameters("FRED") 
  .getAs(Integer.class) 
  .doOnNext(x -> System.out.println("emitted on " + Thread.currentThread().getName())) 
  .subscribe();

System.out.println("second query submitted");

wait for stuff to happen asynchronously
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


Large objects support
------------------------------
Blob and Clobs are straightforward to handle.

### Insert a Clob
Here's how to insert a String value into a Clob (*document* column below is of type ```CLOB```):
```java
String document = ...
Observable<Integer> count = db
		.update("insert into person_clob(name,document) values(?,?)")
		.parameter("FRED")
		.parameter(Database.toSentinelIfNull(document)).count();
```
(Note the use of the ```Database.toSentinelIfNull(String)``` method to handle the null case correctly)

or using a ```java.io.Reader```:
```java
Reader reader = ...;
Observable<Integer> count = db
		.update("insert into person_clob(name,document) values(?,?)")
		.parameter("FRED")
		.parameter(reader).count();
```
### Insert a Null Clob
This requires *either* a special call (```parameterClob(String)```) to identify the parameter as a CLOB:
```java
Observable<Integer> count = db
		.update("insert into person_clob(name,document) values(?,?)")
		.parameter("FRED")
		.parameterClob(null).count();
```
or use the null Sentinel object for Clobs:
```java
Observable<Integer> count = db
		.update("insert into person_clob(name,document) values(?,?)")
		.parameter("FRED")
		.parameter(Database.NULL_CLOB).count();
```
or wrap the String parameter with ```Database.toSentinelIfNull(String)``` as above in the Insert a Clob section.

### Read a Clob
```java
Observable<String> document = db.select("select document from person_clob")
				.getAs(String.class);
```
or
```java
Observable<Reader> document = db.select("select document from person_clob")
				.getAs(Reader.class);
```
### Insert a Blob
Similarly for Blobs (*document* column below is of type ```BLOB```):
```java
byte[] bytes = ...
Observable<Integer> count = db
		.update("insert into person_blob(name,document) values(?,?)")
		.parameter("FRED")
		.parameter(Database.toSentinelIfNull(bytes)).count();
```
### Insert a Null Blob
This requires *either* a special call (```parameterBlob(String)``` to identify the parameter as a CLOB:
```java
Observable<Integer> count = db
		.update("insert into person_blob(name,document) values(?,?)")
		.parameter("FRED")
		.parameterBlob(null).count();
```
or use the null Sentinel object for Blobs:
```java
Observable<Integer> count = db
		.update("insert into person_clob(name,document) values(?,?)")
		.parameter("FRED")
		.parameter(Database.NULL_BLOB).count();
```
or wrap the byte[] parameter with ```Database.toSentinelIfNull(byte[])``` as above in the Insert a Blob section.

### Read a Blob
```java
Observable<byte[]> document = db.select("select document from person_clob")
				.getAs(byte[].class);
```
or
```java
Observable<InputStream> document = db.select("select document from person_clob")
				.getAs(InputStream.class);
```

Returning generated keys
-------------------------
If you insert into a table that say in h2 is of type `auto_increment` then you don't need to specify a value but you may want to know what value was inserted in the generated key field.

Given a table like this
```
create table note(
    id bigint auto_increment primary key,
    text varchar(255)
)
```
This code inserts two rows into the *note* table and returns the two generated keys:

```java
Flowable<Integer> keys = 
    db.update("insert into note(text) values(?)")
      .parameters("hello", "there")
      .returnGeneratedKeys()
      .getAs(Integer.class)
      .blockingSubscribe();
```

The `returnGeneratedKeys` method also supports returning multiple keys per row so the builder offers methods just like `select` to do explicit mapping or auto mapping.

Transactions
-----------------
Transactions are a critical feature of relational databases. 

When we're talking RxJava we need to consider the behaviour of individual JDBC objects when called by different threads, possibly concurrently. The approach taken by *rxjava2-jdbc* outside of a transaction safely uses Connection pools (in a non-blocking way). Inside a transaction we must make all calls to the database using the same Connection object so the behaviour of that Connection when called from different threads is important. Some JDBC drivers provide thread-safety on JDBC objects by synchronizing every call.

The safest approach with transactions is to perform all db interaction synchronously. Asynchronous processing within transactions was problematic in *rxjava-jdbc* because `ThreadLocal` was used to hold the Connection. Asynchronous processing with transactions *is* possible with *rxjava2-jdbc* but should be handled with care given that your JDBC driver may block or indeed suffer from race conditions that most users don't encounter.

Let's look at some examples. The first example uses a transaction across two select statement calls:

```java
Database.test()
  .select("select score from person where name=?") 
  .parameters("FRED", "JOSEPH") 
  .transacted() 
  .getAs(Integer.class) 
  .blockingForEach(tx -> 
    System.out.println(tx.isComplete() ? "complete" : tx.value()));
```

Output:
```
21
34
complete
```

Note that the commit/rollback of the transaction happens automatically.

What we see above is that each emission from the select statement is wrapped with a Tx object including the terminal event (error or complete). This is so you can for instance perform an action using the same transaction. 

Let's see another example that uses the `Tx` object to update the database. We are going to do something a bit laborious that would normally be done in one update statement (`update person set score = -1`) just to demonstrate usage:

```java
Database.test()
  .select("select name from person") 
  // don't emit a Tx completed event
  .transactedValuesOnly() 
  .getAs(String.class) 
  .flatMap(tx -> tx
    .update("update person set score=-1 where name=:name") 
    .parameter("name", tx.value()) 
    // don't wrap value in Tx object 
    .valuesOnly() 
    .counts()) 
  .toList()
  .blockingForEach(System.out::println);
```

Output:
```
[1, 1, 1]

```

Logging
-----------------
Logging is handled by slf4j which bridges to the logging framework of your choice. Add
the dependency for your logging framework as a maven dependency and you are sorted. See the test scoped log4j example in [rxjava2-jdbc/pom.xml](https://github.com/davidmoten/rxjava2-jdbc/blob/master/pom.xml).


