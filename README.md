# pard
Parallel Database Running like a Leopard

## TODO List
### LEVEL 1
1. Add support for vertical partition in table creation.
2. Add support for DELETE.
3. Add support for LOAD.
4. Rule based query optimization.
5. Refactor exchange server and client.
6. Refactor execution framework.
7. Add support for JOIN.

### LEVEL 2
1. Serializaton and deSerialization of `Task` and `PardResultSet`.

## Contribution Guide
#### Recommended Environment
Git + Intellij IDEA + Java8 + Maven3.3.9+
#### Compilation Without Running Unit Tests
`mvn clean package -DskipTests` or `mvn clean compile -DskipTests`
#### Compilation With Running Unit Tests
`mvn clean package` or `mvn clean compile`

#### Tips
1. Compile locally to ensure everything is ok before pushing to Github.
2. Pay attention to CheckStyle. Make sure your code style satisfies the code style rules.
