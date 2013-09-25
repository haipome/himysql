himysql
=======

A mysql c client wrapper, support auto connect and asynchronous write. 

API
---

It uses an high level printf-alike API int order to make it much easier to write sql statement. It support binary data, and auto escape string.

```
    CREATE TABLE user (name varchar(100), age int, addr blob(100));
```

```c
    himysql(c, "insert into user (name, age, addr) values (%s, %d, %b)", \
        "jack", 23, "New York", (size_t)8);
```

Asynchronous write only
-----------------------

This feature is userful when you only need execute write only sql statement such as insert, update, and delete, you don't care the result much, and don't want the synchronous operation slow down the process. 

To use this feature, put HM_USE_THREAD on the flag field when call himysql_init.

```c
    himysql_init("localhost", 0, NULL, NULL, NULL, NULL, HM_USE_THREAD);
```

It create a thread to process the sql statement. When you call himysql or himysql_query, it only put the sql statement in a list, and inform the thread throw a pipe. 

When the mysql connection lost, it will reconnect autoly and redo the sql statement.

-----------------------

More detail see the test and source code.



