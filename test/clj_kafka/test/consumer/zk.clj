(ns clj-kafka.test.consumer.zk
  (:use [expectations]
        [clj-kafka.core :only (with-resource to-clojure)]
        [clj-kafka.producer :only (producer send-messages message)]
        [clj-kafka.test.utils :only (with-test-broker)])
  (:require [clj-kafka.consumer.zk :as zk]))

(def producer-config {"metadata.broker.list" "localhost:9999"
                      "serializer.class" "kafka.serializer.StringEncoder"
                      "partitioner.class" "clj_kafka.test.RoundRobinPartitioner"})

(def test-broker-config {:zookeeper-port 2182
                         :kafka-port 9999
                         :topic "multi-partition-topic"
                         :topic-options {:partitions 2}})

(def consumer-config {"zookeeper.connect" "localhost:2182"
                      "group.id" "clj-kafka.test.consumer"
                      "auto.offset.reset" "smallest"
                      "auto.commit.enable" "false"})

(defn string-value
  [k]
  (fn [m]
    (String. (k m) "UTF-8")))

(defn test-message
  [value]
  (message "multi-partition-topic" "key" value))

(defn send-and-receive
  [messages]
  (with-test-broker test-broker-config
    (with-resource [c (zk/consumer consumer-config)]
      zk/shutdown
      (let [p (producer producer-config)]
        (send-messages p messages)
        (ffirst (map zk/messages (zk/message-streams c "multi-partition-topic" :threads 2)))))))

(println (send-and-receive [(test-message "Hello, world")
                            (test-message "Hello, other world")])))

(given (send-and-receive [(test-message "Hello, world")
                          (test-message "Hello, other world")])
       (expect :topic "multi-partition-topic"
               :offset 0
               :partition 0
               (string-value :value) "Hello, world"))
