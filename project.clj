(defproject org.clojars.sorenmacbeth/cascading.hbase "1.2.9-SNAPSHOT"
  :repositories {"conjars.org" "http://conjars.org/repo"}
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [cascading/cascading-hadoop "2.0.6"
                  :exclusions [org.apache.hadoop/hadoop-core]]
                 [cascading/cascading-local "2.0.6"]
                 [cascading/cascading-test "2.0.6"
                  :exclusions [junit/junit]]
                 [org.apache.thrift/libthrift "0.8.0"]
                 [xerces/xercesImpl "2.9.1"]]
  :profiles {:provided
             {:dependencies
              [[org.apache.hadoop/hadoop-core "0.20.2-dev"]
               [org.apache.hbase/hbase "0.94.1"
                :exclusions [org.apache.thrift/libthrift org.apache.hadoop/hadoop-core]]]}
             :dev
             {:dependencies
              [[junit/junit "4.9"]
               [junit-addons/junit-addons "1.4"]]}}
  :java-source-paths ["src/main/java"]
  :min-lein-version "2.0.0")