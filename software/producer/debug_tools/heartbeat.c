/*
 * Heartbeat Message Kafka Producer
 *
 * Author: Yang Wang <wang701@purdue.edu>
 *
 * Copyright (C) 2018 Purdue University
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <signal.h>
#include <stdbool.h>
#include <errno.h>

#include <sys/time.h>

#include <avro.h>
#include <librdkafka/rdkafka.h>

volatile char key[100];
static char *brokers = "localhost:9092";
static char *ns_cmd = "qmicli -p -d /dev/cdc-wdm0 --nas-get-signal-strength | \
  head -n 3 | tail -n 1 | sed \"s/^.*'\\([-0-9]*\\) dBm'[^']*$/\\1/\"";
static char *led4_cmd = "cat /sys/class/leds/LED_4_GREEN/brightness";
static char *led5_cmd = "cat /sys/class/leds/LED_5_GREEN/brightness";

const char D_HB_SCHEMA[] =
"{\"type\":\"record\",\
  \"name\":\"dhb\",\
  \"fields\":[\
  {\"name\": \"timestamp\", \"type\": \"double\"},\
  {\"name\": \"cellns\", \"type\": \"int\"},\
  {\"name\": \"wifins\", \"type\": \"int\"},\
  {\"name\": \"netled\", \"type\": \"boolean\"},\
  {\"name\": \"statled\", \"type\": \"boolean\"}]}";

/* Timer handler */
void timer_handler(int signum) {
  /* Kafka static variables */
  static rd_kafka_t *rk = NULL;
  static rd_kafka_topic_t *rkt = NULL;
  static rd_kafka_conf_t *conf = NULL;
  static rd_kafka_topic_conf_t *topic_conf = NULL;
  static char errstr[512];

  /* Avro static variables */
  static avro_writer_t writer = NULL;
  static avro_schema_t d_hb_schema = NULL;
  static avro_datum_t d_hb = NULL;
  static char buf[100];

  /* timeval struct */
  struct timeval tp;
  double timestamp;

  /* Heartbeat message variables */
  int cell_ns = -80;
  int wifi_ns = -70; //TODO: get real wifi rssi
  int ret;

  bool netled = false;
  bool statled = false;
  int ledval = 0;

  /* File pointer for running commands */
  FILE *fn;

  /* Broker conf */
  if (conf == NULL) {
    conf = rd_kafka_conf_new();
    /* Kafka conf */
    rd_kafka_conf_set(conf, "batch.num.messages", "20000", errstr,
      sizeof(errstr));
    rd_kafka_conf_set(conf, "queue.buffering.max.messages", "1000000", errstr,
      sizeof(errstr));
    rd_kafka_conf_set(conf, "queue.buffering.max.ms", "1", errstr,
      sizeof(errstr));
    rd_kafka_conf_set(conf, "log.connection.close", "false", errstr,
      sizeof(errstr));
  }

  /* Kafka topic conf */
  if (topic_conf == NULL) {
    topic_conf = rd_kafka_topic_conf_new();
    rd_kafka_topic_conf_set(topic_conf, "request.required.acks", "0", errstr,
      sizeof(errstr));
  }

  /* Create Kafka producer */
  if (rk == NULL) {
    if (!(rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr)))) {
      fprintf(stderr, "%% Failed to create new producer: %s\n", errstr);
    }
  }

  if (rd_kafka_brokers_add(rk, brokers) == 0) {
    fprintf(stderr, "%% No valid brokers specified\n");
    exit(EXIT_FAILURE);
  }

  if (rkt == NULL) {
    /* Create new Kafka topic */
    rkt = rd_kafka_topic_new(rk, "debug", topic_conf);
  }

  /* Initialize the schema structure from JSON */
  if (d_hb_schema == NULL) {
    if (avro_schema_from_json_literal(D_HB_SCHEMA, &d_hb_schema)) {
      fprintf(stderr, "Unable to parse d_hb schema\n");
      exit(EXIT_FAILURE);
    }
  }

  /* Create avro writer */
  if (writer == NULL) {
    writer = avro_writer_memory(buf, sizeof(buf));
  }

  /* Create avro record based on the schema */
  if (d_hb == NULL) {
    d_hb = avro_record(d_hb_schema);
  }

  /* Get UNIX timestamp */
  gettimeofday(&tp, NULL);
  timestamp = tp.tv_sec + tp.tv_usec / 1000000.0;

  /* Check if ISOBlue has Internet */
  ret = system("wget -q --spider http://google.com");
  if (ret == -1) {
    perror("system");
    exit(EXIT_FAILURE);
  }

  /* Change LED4 and 5 status based on Internet connectivity */
  if (WEXITSTATUS(ret) == 0) {
    /* Indicate online */
    system("echo 0 > /sys/class/leds/LED_4_RED/brightness");
    system("echo 255 > /sys/class/leds/LED_4_GREEN/brightness");
#if DEBUG
    printf("%f: alive\n", timestamp);
    fflush(stdout);
#endif
  } else {
    /* Indicate offline */
    system("echo 0 > /sys/class/leds/LED_4_GREEN/brightness");
    system("echo 255 > /sys/class/leds/LED_4_RED/brightness");
#if DEBUG
    printf("%f: dead\n", timestamp);
    fflush(stdout);
#endif
  }

  /* Get the network strength from command */
  fn = popen(ns_cmd, "r");
  if (fn != NULL) {
    fscanf(fn, "%d", &cell_ns);
  } else {
    perror("popen");
    exit(EXIT_FAILURE);
  }

  /* Close the subprocess */
  if (pclose(fn) < 0) {
    perror("pclose");
    exit(EXIT_FAILURE);
  }

  printf("%f: cell network strength is %d\n", timestamp, cell_ns);
  fflush(stdout);

  if (cell_ns < -100) {
    printf("%f: Network strength %d dBm doesn't make sense! Something WRONG!\n",
      timestamp, cell_ns);
  }

  /* Check if LED4 is lit green */
  fn = popen(led4_cmd, "r");
  if (fn != NULL) {
    fflush(stdout);
    fscanf(fn, "%d", &ledval);
    printf("led4val: %d\n", ledval);
    if (ledval == 255) {
      netled = true;
    } else {
      netled = false;
    }
    printf("netled: %d\n", netled);
  } else {
    perror("popen");
    exit(EXIT_FAILURE);
  }

  /* Close the subprocess */
  if (pclose(fn) < 0) {
    perror("pclose");
    exit(EXIT_FAILURE);
  }
  /* Check if LED5 is lit green */
  fn = popen(led5_cmd, "r");
  if (fn != NULL) {
    fscanf(fn, "%d", &ledval);
    printf("led5val: %d\n", ledval);
    if (ledval == 255) {
      statled = true;
    } else {
      statled = false;
    }
    printf("statled: %d\n", statled);
  } else {
    perror("popen");
    exit(EXIT_FAILURE);
  }
  /* Close the subprocess */
  if (pclose(fn) < 0) {
    perror("pclose");
    exit(EXIT_FAILURE);
  }

  /* Construct avro data pieces */
  avro_datum_t ts_datum = avro_double(timestamp);
  avro_datum_t cell_ns_datum = avro_int32(cell_ns);
  avro_datum_t wifi_ns_datum = avro_int32(wifi_ns);
  avro_datum_t netled_datum = avro_boolean(netled);
  avro_datum_t statled_datum = avro_boolean(statled);

  if (avro_record_set(d_hb, "timestamp", ts_datum)
    || avro_record_set(d_hb, "cellns", cell_ns_datum)
    || avro_record_set(d_hb, "wifins", wifi_ns_datum)
    || avro_record_set(d_hb, "netled", netled_datum)
    || avro_record_set(d_hb, "statled", statled_datum)) {
    fprintf(stderr, "Unable to set record to d_hb\n");
    exit(EXIT_FAILURE);
  }

  if (avro_write_data(writer, d_hb_schema, d_hb)) {
    fprintf(stderr, "unable to write d_hb datum to memory\n");
    exit(EXIT_FAILURE);
  }

//  printf("the key is: %s\n", key);

  if (rd_kafka_produce(rkt, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_COPY,
      buf, avro_writer_tell(writer), key, strlen(key), NULL) == -1) {
    fprintf(stderr, "%% Failed to produce to topic %s "
            "partition %i: %s\n",
            rd_kafka_topic_name(rkt), RD_KAFKA_PARTITION_UA,
            rd_kafka_err2str(rd_kafka_last_error()));
    rd_kafka_poll(rk, 0);
  }

  rd_kafka_poll(rk, 0);

  /* Decrement all our references to prevent memory from leaking */
  avro_datum_decref(ts_datum);
  avro_datum_decref(cell_ns_datum);
  avro_datum_decref(wifi_ns_datum);
  avro_datum_decref(netled_datum);
  avro_datum_decref(statled_datum);

  /* Reset the writer */
  avro_writer_reset(writer);
}

int main(int argc, char *argv[]) {
  if (argv[1] == NULL) {
    fprintf(stderr, "You need to supply ISOBlue ID file\n");
    return EXIT_FAILURE;
  }

  /* fp for opening uuid file */
  FILE *fp;
  char *id = 0;
  long length;

  /* Timer stuff variables */
  struct sigaction sa;
  struct itimerval timer;

  /* Get the id */
  fp = fopen(argv[1], "r");
  if (fp) {
    fseek(fp, 0, SEEK_END);
    length = ftell(fp);
    fseek(fp, 0, SEEK_SET);
    id = malloc(length);
    if (id) {
      fread(id, 1, length, fp);
      if (id[length - 1] == '\n') {
        id[--length] = '\0';
      }
    }
    fclose(fp);
  } else {
    perror("ISOBlue ID file");
    return EXIT_FAILURE;
  }

  /* Create the key */
  strcpy(key, "hb");
  strcat(key, ":");
  strcat(key, id);

  printf("the key is: %s\n", key);

  /* Install timer_handler as the signal handler for SIGALRM. */
  memset(&sa, 0, sizeof(sa));
  sa.sa_handler = &timer_handler;
  sigaction(SIGALRM, &sa, NULL);

  /* Configure the timer to expire after 5 secs... */
  timer.it_value.tv_sec = 5;
  timer.it_value.tv_usec = 0;
  /* ... and every 10 secs after that. */
  timer.it_interval.tv_sec = 5;
  timer.it_interval.tv_usec = 0;

  /* Start a real timer. It counts down whenever this process is
   * executing. */
  setitimer(ITIMER_REAL, &timer, NULL);

  system("echo 255 > /sys/class/leds/LED_4_RED/brightness");

  while (1) {
    sleep(1);
  }

  return EXIT_SUCCESS;
}
