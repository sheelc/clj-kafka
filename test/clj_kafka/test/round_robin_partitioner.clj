(ns clj-kafka.test.round-robin-partitioner
  (:gen-class
    :name clj_kafka.test.RoundRobinPartitioner
    :init init
    :state state
    :implements [kafka.producer.Partitioner]
    :constructors {[kafka.utils.VerifiableProperties] []}))

(defn -init
  [props]
  [[] (atom -1)])

(defn -partition
  [this partition-key partition-count]
  (swap! (.state this) inc)
  (println @(.state this))
  (println partition-count)
  (mod  @(.state this) partition-count))
