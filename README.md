himysql
=======

A printf like mysql c client wrapper

API
---

It uses an high level printf-alike API int order to make it much easier to write sql statement. It support binary data, and auto escape string.

```
    CREATE TABLE user (name varchar(100), age int, addr blob(100));
```

```c
    himysql(c, "insert into user (name, age, addr) values (%s, %d, %b)", "jack", 23, "New York", (size_t)8);
```

-----------------------

More detail see the test and source code.

