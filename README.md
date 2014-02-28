MySQL DB Patterns
=================
by [Andrew Brampton](http://bramp.net) 2013

Intro
-----

Some useful java code backed by JDBC that implements some common patterns.

So far a Condition object, and a Queue are implemented.

```maven
	<dependency>
		<groupId>net.bramp.db-patterns</groupId>
		<artifactId>db-patterns</artifactId>
		<version>0.1</version>
	</dependency>
```

Condition
---------

A distributed Java Condition

```java
  DataSource ds = ...
  Condition condition = new MySQLSleepBasedCondition(ds, "lockname");
  condition.await(); // Blocks until a notify
  
  // on another thread (or process, or machine)
  condition.notify();
```

The MySQLSleepBasedCondition is based on the MySQL ``SLEEP()`` and ``KILL QUERY``

The thread that is woken up is guaranteed to be the one that has waited the longest.


Queue
-----

A distributed MySQL backed Java BlockingQueue

```java
  DataSource ds = ...
  BlockingQueue<String> queue = new MySQLBasedQueue<String>(ds, "queue name", String.class);
  queue.add("Some String");
  
  // on another thread (or process, or machine)
  String s = queue.poll(); // Non blocking
  // or
  String s = queue.take(); // Blocks until element available
```

The MySQLBasedQueue uses the MySQLSleepBasedCondition to help form a blocking
queue, that can work without polling the database for new work.


Useful Articles
---------------
	https://blog.engineyard.com/2011/5-subtle-ways-youre-using-mysql-as-a-queue-and-why-itll-bite-you
