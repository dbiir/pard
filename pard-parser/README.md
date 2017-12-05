## SQL Parser
Pard parser is to parse a SQL string into an abstract syntax tree.
And apply semantic checks on it.\
To use the parser, create an object of `SqlParser`, and call the method `createStatement`.
An `Statement` object is returned as the parsing result.\
During parsing, 

### SQL SYNTAX
#### HORIZONTAL PARTITION
We support three kinds of horizontal partitions.
1. Range partition.
2. List partition.
3. Hash partition.
##### RANGE PARTITION
Example:
```sql
CREATE TABLE orders_range
(
id INT PRIMARY KEY,
name VARCHAR(30),
order_date DATE
) PARTITION BY RANGE
(
p0 id LESS THAN 5 AND order_date GREATEREQ THAN '2017-01-01' ON node0,
p1 id LESS THAN 10 ON node1,
p2 id LESS THAN 15 ON node2,
p3 id LESS THAN MAXVALUE
);
```
##### LIST PARTITION
Example:
```sql
CREATE TABLE orders_range
(
id INT PRIMARY KEY,
name VARCHAR(30),
order_date DATE
) PARTITION BY LIST
(
p0 id IN (1, 2, 3) AND name IN ('alice', 'bob') ON node0,
p1 id IN (4, 5) ON node1,
p2 id IN (8, 9, 10)
);
```
##### HASH PARTITION
```sql
CREATE TABLE orders_range
(
id INT PRIMARY KEY,
name VARCHAR(30),
order_date DATE
) PARTITION BY HASH(id) PARTITIONS 4;
```

#### VERTICAL PARTITIONS
```sql
CREATE TABLE orders_range
(id INT PRIMARY KEY, name VARCHAR(30)) ON node1,
(id INT PRIMARY KEY, order_date DATE)
```
