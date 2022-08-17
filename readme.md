1. start database(use GTID)

mysqlcnf/mysql.cnf
```
[mysqld]
server-id = 1
gtid_mode=ON
enforce-gtid-consistency=ON

skip-host-cache
skip-name-resolve
```

```
docker run -itd \
       -p 3306:3306 \
       -v $PWD/mysqlcnf:/etc/mysql/conf.d \
       -v $PWD/mysqldata:/var/lib/mysql \
       -e MYSQL_ROOT_PASSWORD="123456" \
       mysql:8.0.13
```

2. create user
```
CREATE USER 'slave_user'@'%' IDENTIFIED WITH caching_sha2_password BY 'slave_password';
GRANT REPLICATION SLAVE ON *.* TO 'slave_user'@'%';
GRANT SELECT ON *.* TO 'slave_user'@'%';
flush privileges;
```
