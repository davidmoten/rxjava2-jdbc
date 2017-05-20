# rxjava2-jdbc

With the release of RxJava 2 now is a good time for a rewrite of [rxjava-jdbc](https://github.com/davidmoten/rxjava-jdbc). 

See [wiki](https://github.com/davidmoten/rxjava2-jdbc/wiki)

Status: *in development*

Non-blocking connection pools
-------------------------------
A new exciting feature of *rxjava2-jdbc* is the availability of non-blocking connection pools. 

In normal non-reactive database programming a couple of different threads (started by servlet calls for instance) will *race* for the next available connection from a pool of database connections. If no unused connection remains in the pool then the standard non-reactive approach is to **block the thread** until a connection becomes available. This is a resource issue as each blocked threads hold onto ~0.5MB of stack and may incur a context switch (adds latency to thread processing).

`rxjava-jdbc2` uses non-blocking JDBC connection pools by default (but is configurable to use whatever you want). 

The simplest way of creating a `Database` instance with a non-blocking connection pool is:

```java
Database db = Database.from(url, maxPoolSize);
```

If you want to play with the in-built test database (requires Apache Derby dependency) then:

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




