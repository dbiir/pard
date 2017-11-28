## SQL Parser
### SQL SYNTAX
#### PARTITION
We support three kinds of partitions.
1. Range partition.
2. List partition.
3. Hash partition.
##### RANGE PARTITION
```sql
CREATE TABLE orders_range
(
id INT PRIMARY KEY,
name VARCHAR(30),
order_date DATE,
) PARTITION BY RANGE(id)
(
p0 VALUES LESS THAN (5),
p1 VALUES LESS THAN (10),
p2 VALUES LESS THAN (15),
p3 VALUES LESS THAN (MAXVALUE)
);
```
##### LIST PARTITION
```sql
CREATE TABLE orders_range
(
id INT PRIMARY KEY,
name VARCHAR(30),
order_date DATE,
) PARTITION BY LIST(id)
(
p0 VALUES IN (1, 2, 3),
p1 VALUES IN (4, 5),
p2 VALUES IN (8, 9, 10)
);
```
##### HASH PARTITION
```sql
CREATE TABLE orders_range
(
id INT PRIMARY KEY,
name VARCHAR(30),
order_date DATE,
) PARTITION BY HASH(id) PARTITIONS 4;
```