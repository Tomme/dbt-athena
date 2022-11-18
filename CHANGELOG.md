## v1.3.1

### Features
* Added table materialization for Iceberg
* Override datediff and dateadd functions to support metrics

## v1.3.0

### Features
* Update `dbt-core` to 1.3.0

## v1.0.4

### Bugfixes
* Add support for partition fields of type timestamp
* Use correct escaper for INSERT queries
* Share same boto session between every calls

### Features
* Get model owner from manifest

## v1.0.3
* Fix issue on fetching partitions from glue, using pagination
