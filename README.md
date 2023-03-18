# Pata

*Warning: This is a prototype. The server component may crash and take your database with it!*

Pata (female duck in Spanish or close friend in Perǘ) is a JDBC driver for DuckDB (www.duckdb.org). It allows to interact with the database from another process. As DuckDB is an embedded database it is not easily possible to interact with a running process from outside.

Pata tries to solve this by adding a server component to the running database process and providing a client component to connect to the server. In this way you could connect with for example DBeaver to a running DuckDB database.

## Usage

### Dependecies

Include the pata-x.y.z.jar into the database process. It has following dependencies (from build.gradle), that must be available:
>    api 'org.apache.arrow:arrow-vector:10.0.1'  
>    api 'org.apache.arrow:arrow-memory-core:10.0.1'  
>    api 'org.apache.arrow:arrow-memory-netty:10.0.1'  
>    api 'com.fasterxml.jackson.core:jackson-core:2.14.1'  
>    api 'org.duckdb:duckdb_jdbc:0.7.1'  

### JVM Parameter

Apache Arrow needs following JVM parameter (see https://arrow.apache.org/docs/java/install.html#java-compatibility):

```
--add-opens=java.base/java.nio=ALL-UNNAMED
```

### Starting the server

The database process has to start the server (typically in a separate Thread) like this:
```java
			Server s = new Server((DuckDBConnection) con, 41442);		
			s.startServer();
```

The client needs the same dependencies.
*It can only connect **locally**. There is no user management/security/encryption. Take this into account before starting up a server.*

## DBeaver usage

A generic driver can be used. Class name would be 'duckdb_driver.pata.jdbc.PataDriver' and the connection URL 'jdbc:duckdb-pata:41442'. The port can be adjusted on both ends.

Make sure to include all dependencies!
