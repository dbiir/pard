# pard
Parallel Database Running like a Leopard

## TODO List
1. Serializaton and deSerialization of `Task` and `PardResultSet`.
2. Add support for vertical partition in table creation.
3. Rule based query optimization.
4. Refactor execution framework.
5. Add support for JOIN.

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
