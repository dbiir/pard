# pard
Parallel Database Running like a Leopard

## TODO List
### LEVEL 1
1. Add support for vertical partition in table creation.  @hanhan
2. Add support for DELETE.  @jishen
3. Add support for LOAD.  @guod
4. Query logical optimization.  @hanhan
5. Branches pruning for plan.  @hanhan, guod
6. Refactor exchange server and client.  @guod
7. Refactor execution framework.  @guod
8. Add support for JOIN.  @guod
9. Join optimization.

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
