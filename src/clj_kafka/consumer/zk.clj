(ns clj-kafka.consumer.zk
  (:import [kafka.consumer ConsumerConfig Consumer KafkaStream]
           [kafka.javaapi.consumer ConsumerConnector])
  (:use [clj-kafka.core :only (as-properties to-clojure with-resource)])
  (:require [zookeeper :as zk]))

(defn consumer
  "Uses information in Zookeeper to connect to Kafka. More info on settings
   is available here: http://incubator.apache.org/kafka/configuration.html

   Recommended for using with with-resource:
   (with-resource [c (consumer m)]
     shutdown
     (take 5 (messages c \"test\")))

   Keys:
   zookeeper.connect             : host:port for Zookeeper. e.g: 127.0.0.1:2181
   group.id                      : consumer group. e.g. group1
   auto.offset.reset             : what to do if an offset is out of range, e.g. smallest, largest
   auto.commit.interval.ms       : the frequency that the consumed offsets are committed to zookeeper.
   auto.commit.enable            : if set to true, the consumer periodically commits to zookeeper the latest consumed offset of each partition"
  [m]
  (let [config (ConsumerConfig. (as-properties m))]
    (Consumer/createJavaConsumerConnector config)))

(defn shutdown
  "Closes the connection to Zookeeper and stops consuming messages."
  [^ConsumerConnector consumer]
  (.shutdown consumer))

(defn- lazy-iterate
  [it]
  (lazy-seq
   (when (.hasNext it)
     (cons (.next it) (lazy-iterate it)))))

(defn messages
  "Creates a sequence of KafkaMessage messages from the given stream."
  [^KafkaStream stream]
  (map to-clojure
       (lazy-iterate (.iterator ^KafkaStream stream))))

(defn message-streams
  "Creates a sequence of sequences of KafkaMessage messages from the given
   topic. The number of KafkaMessages sequences is determined by the number of
   threads passed in, or defaults to 1."
  [^ConsumerConnector consumer topic & {:keys [threads]
                                        :or   {threads 1}}]
  (->> (.createMessageStreams consumer {topic (int threads)})
       (vals)
       (first)
       (map messages)))
