## v0.6.3 (2024-10-18)

### Fix

- include utils as a package

## v0.6.2 (2024-10-16)

### Refactor

- apply ruff fix and format so CI tests pass
- use ruff for lint and formatting (remove black)

## v0.6.1 (2024-10-03)

### Fix

- KafkaReader and TableReader should inherit from base Reader class

### Refactor

- accept type[Writer] for main Sparkle interface

## v0.6.0 (2024-10-03)

### Feat

- **kafkaWriter**: add batch publisher
- add chispa pkg for easy spark DF comparisons
- add kafka-ui service so I can see what goes on

### Fix

- add missing colon
- check if Kafka columns in writer are *exactly* {key, value}

## v0.5.1 (2024-10-02)

### Fix

- regression on #21.
- package version in the package.

## v0.5.0 (2024-09-30)

### Fix

- not supported environment exception

### Refactor

- rename kafka_options to kafka_spark_options
- rename local to generic
- accept readers in Sparkle class
- delete local spark session
- Spark configuration
- let Sparke create Spark session

## v0.4.0 (2024-09-16)

### Feat

- wait for schema registry to become available
- add Kafka Avro reader
- add Kafka Avro reader
- add avro parser
- add schema registry class

### Fix

- start from earliest checkpoint
- use colima on mac
- wait for schema registry to be ready before running tests

### Refactor

- schema type to config
- Switch to Poetry from PDM

## v0.3.1 (2024-09-11)

### Fix

- **spark**: lock package version, to ensure they match plugins.
- **typing**: error "AnalysisException" has no attribute "desc"

### Refactor

- **test**: run development services using devenv.
- switch to poetry.

## v0.3.0 (2024-09-10)

### Feat

- Add Kafka stream publisher
- add dataframe to kafka compatible dataframe transforner

## v0.2.0 (2024-09-06)

### Feat

- add table reader

### Refactor

- iceberg config name

## v0.1.2 (2024-09-06)

### Fix

- set minium pyspark version instead of locking to specific version
- set minimum python version instead of locking to exact version

## v0.1.1 (2024-09-05)

### Refactor

- **devenv**: downgrade Java to version 8.
- **devenv**: switch to nixpkgs-unstable.

## v0.1.0 (2024-09-05)

### Feat

- add Iceberg writer with tests
- add Sparkle class
- add pyspark to dependencies.
- **sources**: initialize the reader design.
- **config**: initialize the application structure.

### Fix

- do not fail fast CI matrix
- python version
- devenv & python version
- **config**: config implemnetation errors.

### Refactor

- ruff hints
- Read source on reader initialization
- **Sparkle**: to generate input configuration.
- **Soruces**: to preserve typing, documentation and autocompletion.

## 0.0.0 (2024-07-26)
