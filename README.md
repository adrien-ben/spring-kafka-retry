# spring-kafka-retry

[![Build Status](https://travis-ci.org/adrien-ben/spring-kafka-retry.svg?branch=master)](https://travis-ci.org/adrien-ben/spring-kafka-retry)

Spring kafka test application for retries.

## Test

```sh
mvn clean test
```

There are two test cases. One that sends "Foo" into `topic` which triggers
a `RetryableException` throw. The other sends "NotFoo" into `topic` which triggers
a `RuntimeException` throw.
