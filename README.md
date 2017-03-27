# IODB - database engine for blockchain 

[![Build Status](https://travis-ci.org/input-output-hk/iodb.svg?branch=master)](https://travis-ci.org/input-output-hk/iodb)

IODB is persistent key-value store. It is inspired by [RocksDB](http://rocksdb.org).
Main features include

- Ordered key-value store 
- Written in Scala, functional interface
- Multi-threaded background compaction
- Very fast durable commits
- Atomic updates with MVCC isolation and crash protection
- Snapshots with branching and rollbacks
- Log structured storage, old data are never overwritten for improved crash protection

Getting started
---------------------

IODB builds are available in [Maven repository](https://mvnrepository.com/artifact/org.scorexfoundation/iodb_2.12). Maven dependency snippet is bellow, replace `$VERSION` with 
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.scorexfoundation/iodb_2.12/badge.svg)](https://mvnrepository.com/artifact/org.scorexfoundation/iodb_2.12)
:
 
```xml
<dependency>
    <groupId>org.scorexfoundation</groupId>
    <artifactId>iodb_2.12</artifactId>
    <version>$VERSION</version>
</dependency>
```

Code examples are in the [src/test/scala/examples](src/test/scala/examples) folder.

Documentation is in the [doc](doc) folder.

Compile
---------

IODB works with Intellij IDEA with Scala plugin.  

- Checkout IODB: 
```
git clone https://github.com/input-output-hk/iodb.git
```
- Install [SBT](http://www.scala-sbt.org/release/docs/Setup.html)


- Compile IODB and install JAR files into local repository: 
```
sbt publish
```


