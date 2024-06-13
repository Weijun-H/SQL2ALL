<!-- add logo -->

<p align="center">
    <img src="img/logo.png" alt="Image" height="250">
</p>

# SQL2ALL

SQL2ALL is a simple tool that allows you to asynchronically store your SQL queries in any format you like and then run them against any database you like. It is designed to be simple to use and easy to extend.

## Roadmap

Support for the following databases is included:

- [x] MySQL/MariaDB ([mysql_async](https://crates.io/crates/mysql_async))
- [x] PostgreSQL ([tokio-postgres](https://crates.io/crates/tokio-postgres))
- [x] SQLite ([rusqlite](https://crates.io/crates/rusqlite))
- [ ] Graph Database (Neo4j/KuzuDB)

Support for the following output formats is included:

- [x] CSV
- [x] JSON
- [x] Parquet
- [ ] ORC
- [ ] Avro
- [ ] Lance

## Usage

```shell
sql2all -u <url> -o <output> -q <query>

sql2all -u "mysql://root@localhost:3306/test" -o "test.parquet" -q "select * from payment"

sql2all -u "postgresql://root@localhost:5432/test" -o "test.csv" -q "select * from payment"

sql2all -u "sqlite:///test.db" -o "test.json" -q "select * from payment"
```
