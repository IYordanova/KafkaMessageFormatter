package my.custom

import kafka.tools.DefaultMessageFormatter
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.record.TimestampType
import org.apache.kafka.common.serialization.Deserializer

import java.io.PrintStream
import java.nio.charset.StandardCharsets
import Console._

class KafkaMessageFormatter extends DefaultMessageFormatter {


  override def writeTo(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]], output: PrintStream): Unit = {

    def deserialize(deserializer: Option[Deserializer[_]], sourceBytes: Array[Byte], topic: String) = {
      val nonNullBytes = Option(sourceBytes).getOrElse(nullLiteral)
      val convertedBytes = deserializer
        .map(d => utfBytes(d.deserialize(topic, consumerRecord.headers, nonNullBytes).toString))
        .getOrElse(nonNullBytes)
      convertedBytes
    }

    import consumerRecord._

    if (printTimestamp && timestampType != TimestampType.NO_TIMESTAMP_TYPE) {
      output.write(utfBytes(s"$CYAN└── $timestampType:$RESET $timestamp"))
      output.write(lineSeparator)
    }

    if (printPartition) {
      output.write(utfBytes(s"$CYAN└── Partition:$RESET ${partition().toString}"))
      output.write(lineSeparator)
    }

    if (printOffset) {
      output.write(utfBytes(s"$CYAN└── Offset:$RESET ${offset().toString}"))
      output.write(lineSeparator)
    }

    if (printHeaders) {
      if (!headers().toArray.isEmpty) {
        output.write(utfBytes(s"$CYAN└── Headers:$RESET "))
      }
      val headersIt = headers().iterator
      while (headersIt.hasNext) {
        val header = headersIt.next()
        output.write(utfBytes(s"\n\t${header.key()}: "))
        output.write(deserialize(headersDeserializer, header.value(), topic))
        if (headersIt.hasNext) {
          output.write(headersSeparator)
        }
      }
      output.write(lineSeparator)
    }

    if (printKey) {
      output.write(utfBytes(s"$CYAN└── Key:$RESET "))
      output.write(deserialize(keyDeserializer, key, topic))
      output.write(lineSeparator)
    }

    if (printValue) {
      output.write(deserialize(valueDeserializer, value, topic))
      output.write(lineSeparator)
      output.write(utfBytes(s"---------------"))
      output.write(lineSeparator)
    }
  }

  private def utfBytes(str: String) = str.getBytes(StandardCharsets.UTF_8)

}