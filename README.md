# pard
Parallel Database Running like a Leopard

## Development Plan
#### First Round (11.15)
1. SQL Parser
2. Catalog
3. Operator Set
4. Connector Interface
5. Storage Manager Interface
6. Task Interface
7. NodeKeeper Interface

#### Second Round (11.30)
1. Communication
2. Optimizer Framework
3. Connector
4. Catalog
5. Planner and Scheduler Framework

#### Third Round (12.30)
1. Optimizer Cost Model
2. Executor
3. Storage Manager
4. NodeKeeper
5. Job Execution Pipeline

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

## Code Style Rules
#### SPACE
#### IF-ELSE
#### FOR-LOOP
#### TRY-CATCH
#### FUNCTION
#### CLASS/INTERFACE
#### COMMENT
#### IMPORTS
#### VARIABLES AND CONSTANTS
#### NAMING CONVENTION
