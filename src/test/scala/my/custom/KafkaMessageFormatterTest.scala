package my.custom

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.{RecordHeader, RecordHeaders}
import org.apache.kafka.common.record.TimestampType
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.argThat
import org.mockito.Mockito.{doNothing, times, verify}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike
import org.scalatestplus.mockito.MockitoSugar


import scala.jdk.CollectionConverters._
import java.io.PrintStream
import java.util
import Console._

class KafkaMessageFormatterTest extends AnyWordSpecLike with Matchers with MockitoSugar {

  private val message =
    """
      {
        "data": {
          "string_property":"a_string",
          "boolean_property":true,
          "int_property":1046,
          "double_property":248.46,
          "timestamp_property":"2021-12-10 11:30:30.123",
          "some_object_property": {
            "nested_string_property": "another string with spaces and @$%|\|«€# chars"
          }
          "array_property": [
            "one": 1,
            "two": 2,
            "three": 3
          ]
        }
      }
    """.stripMargin

  private val errorHeader =
    """
      error {
        "message": "some error message",
        "timestamp":"2021-12-10 11:30:30.123",
        "source": "the source"
      }
    """.stripMargin

  private val offset = 12


  private def createRecord(): ConsumerRecord[Array[Byte], Array[Byte]] = {
    val header1 = new RecordHeader("header_1", "header 1 value".getBytes())
    val header2 = new RecordHeader("header_2", errorHeader.getBytes())
    new ConsumerRecord[Array[Byte], Array[Byte]](
      "topic",
      0,
      offset,
      1644070402L,
      TimestampType.CREATE_TIME,
      1L,
      1,
      1,
      "key".getBytes(),
      message.getBytes(),
      new RecordHeaders(Array[Header](header1, header2))
    )
  }

  private def initFormatter(configs: util.Map[String, _]) = {
    val formatter = new KafkaMessageFormatter()
    formatter.configure(configs)
    formatter
  }

  "formatting ConsumerRecord with all print.* properties" should {
    "should write them as expected" in {
      val configs = Map(
        "print.timestamp" -> "true",
        "print.key" -> "true",
        "print.offset" -> "true",
        "print.partition" -> "true",
        "print.headers" -> "true",
        "print.value" -> "true",
      ).asJava
      val formatter: KafkaMessageFormatter = initFormatter(configs)

      val printStream = mock[PrintStream]
      val byteArrCaptor: ArgumentCaptor[Array[Byte]] = ArgumentCaptor.forClass(classOf[Array[Byte]])
      doNothing().when(printStream).write(byteArrCaptor.capture())

      val record = createRecord()

      formatter.writeTo(record, printStream)

      verify(printStream, times(20)).write(argThat((_: Array[Byte]) => true))

      val vals = byteArrCaptor.getAllValues
        .stream()
        .map(byteVal => new String(byteVal))
        .collect(java.util.stream.Collectors.toList[String])

      vals.get(0) should be(s"$CYAN└── CreateTime:$RESET 1644070402")
      vals.get(1) should be("\n")

      vals.get(2) should be(s"$CYAN└── Partition:$RESET 0")
      vals.get(3) should be("\n")

      vals.get(4) should be(s"$CYAN└── Offset:$RESET 12")
      vals.get(5) should be("\n")

      vals.get(6) should be(s"$CYAN└── Headers:$RESET ")
      vals.get(7) should be("\n\theader_1: ")
      vals.get(8) should be("header 1 value")
      vals.get(9) should be(",")
      vals.get(10) should be("\n\theader_2: ")
      vals.get(11) should be("\n      error {\n        \"message\": \"some error message\",\n        \"timestamp\":\"2021-12-10 11:30:30.123\",\n        \"source\": \"the source\"\n      }\n    ")
      vals.get(12) should be("\n")

      vals.get(13) should be(s"$CYAN└── Key:$RESET ")
      vals.get(14) should be("key")
      vals.get(15) should be("\n")

      vals.get(16) should be("\n      {\n        \"data\": {\n          \"string_property\":\"a_string\",\n          \"boolean_property\":true,\n          \"int_property\":1046,\n          \"double_property\":248.46,\n          \"timestamp_property\":\"2021-12-10 11:30:30.123\",\n          \"some_object_property\": {\n            \"nested_string_property\": \"another string with spaces and @$%|\\|«€# chars\"\n          }\n          \"array_property\": [\n            \"one\": 1,\n            \"two\": 2,\n            \"three\": 3\n          ]\n        }\n      }\n    ")
      vals.get(17) should be("\n")
    }
  }

  "formatting ConsumerRecord with only couple print.* properties" should {
    "should write them as expected" in {
      val configs = Map(
        "print.partition" -> "true",
        "print.value" -> "true",
      ).asJava
      val formatter: KafkaMessageFormatter = initFormatter(configs)

      val printStream = mock[PrintStream]
      val byteArrCaptor: ArgumentCaptor[Array[Byte]] = ArgumentCaptor.forClass(classOf[Array[Byte]])
      doNothing().when(printStream).write(byteArrCaptor.capture())

      val record = createRecord()

      formatter.writeTo(record, printStream)

      verify(printStream, times(6)).write(argThat((_: Array[Byte]) => true))

      val vals = byteArrCaptor.getAllValues
        .stream()
        .map(byteVal => new String(byteVal))
        .collect(java.util.stream.Collectors.toList[String])

      vals.get(0) should be(s"$CYAN└── Partition:$RESET 0")
      vals.get(1) should be("\n")

      vals.get(2) should be("\n      {\n        \"data\": {\n          \"string_property\":\"a_string\",\n          \"boolean_property\":true,\n          \"int_property\":1046,\n          \"double_property\":248.46,\n          \"timestamp_property\":\"2021-12-10 11:30:30.123\",\n          \"some_object_property\": {\n            \"nested_string_property\": \"another string with spaces and @$%|\\|«€# chars\"\n          }\n          \"array_property\": [\n            \"one\": 1,\n            \"two\": 2,\n            \"three\": 3\n          ]\n        }\n      }\n    ")
      vals.get(3) should be("\n")
    }
  }


  "formatting ConsumerRecord with no properties" should {
    "should write the value" in {
      val configs = Map.empty[String, String].asJava
      val formatter: KafkaMessageFormatter = initFormatter(configs)

      val printStream = mock[PrintStream]
      val byteArrCaptor: ArgumentCaptor[Array[Byte]] = ArgumentCaptor.forClass(classOf[Array[Byte]])
      doNothing().when(printStream).write(byteArrCaptor.capture())

      val record = createRecord()

      formatter.writeTo(record, printStream)

      verify(printStream, times(4)).write(argThat((_: Array[Byte]) => true))

      val vals = byteArrCaptor.getAllValues
        .stream()
        .map(byteVal => new String(byteVal))
        .collect(java.util.stream.Collectors.toList[String])

      vals.get(0) should be("\n      {\n        \"data\": {\n          \"string_property\":\"a_string\",\n          \"boolean_property\":true,\n          \"int_property\":1046,\n          \"double_property\":248.46,\n          \"timestamp_property\":\"2021-12-10 11:30:30.123\",\n          \"some_object_property\": {\n            \"nested_string_property\": \"another string with spaces and @$%|\\|«€# chars\"\n          }\n          \"array_property\": [\n            \"one\": 1,\n            \"two\": 2,\n            \"three\": 3\n          ]\n        }\n      }\n    ")
      vals.get(1) should be("\n")
    }
  }


  "formatting ConsumerRecord with print.value=false property" should {
    "should not write the value" in {
      val configs = Map("print.value" -> "false").asJava
      val formatter: KafkaMessageFormatter = initFormatter(configs)

      val printStream = mock[PrintStream]
      val byteArrCaptor: ArgumentCaptor[Array[Byte]] = ArgumentCaptor.forClass(classOf[Array[Byte]])
      doNothing().when(printStream).write(byteArrCaptor.capture())

      val record = createRecord()

      formatter.writeTo(record, printStream)

      verify(printStream, times(0)).write(argThat((_: Array[Byte]) => true))
    }
  }

}
