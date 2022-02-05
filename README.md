# KafkaMessageFormatter

### Description

This is a tiny scala project with the sole purpose of providing a custom message formatter used by the kafka `ConsoleConsumer`.


### Initial Setup

- Clone the repo
- build the jar (i.e run `sbt assembly`)
- copy or move the resulting jar from <project>/target/jars to the libs dir of your local kafka install (should be something like **/usr/local/Cellar/kafka/2.6.0_1/libexec/libs/**)

### Usage

On the command line run the standard consumer and pass `--formatter my.custom.KafkaMessageFormatter` 
along with the properties accepted by kafka 

- **timestamp** - not printed by default, to enable use `--property print.timestamp=true`
- **partition** - not printed by default, to enable use `--property print.partition=true`
- **offset** - not printed by default, to enable use `--property print.offset=true`
- **headers** - not printed by default, to enable use `--property print.headers=true`
- **key** - not printed by default, to enable use `--property print.key=true`
- **value** - printed by default unless explicitly disabled `--property print.value=false`

Example:
```
kafka-console-consumer  \
 --bootstrap-server localhost:9092   \
 --topic my_topic  \
 --formatter my.custom.KafkaMessageFormatter \
 --property print.headers=true
```