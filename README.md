# Dataflow Pub/Sub Event Router

A pipeline which re-publishes events to different topics based a message attribute. The
event time is saved to an additional attribute, "ts", in the output so the next consumer
can use the `PubsubIO.withTimestampAttribute(..)` to automatically place the message in
the correct window.

## Getting Started

### Requirements

* Java 8
* Maven 3

### Building the Project

Build the entire project using the maven compile command.
```sh
mvn clean && mvn compile
```
