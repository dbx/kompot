(defproject hu.dbx/kompot-gui "1.0.0-SNAPSHOT"
  :description "Graphical interface for kompot events"
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [mount "0.1.13"]
                 [hiccup "1.0.5"]
                 [http-kit "2.2.0"]
                 [hu.dbx/kompot "0.1.31"]
                 [io.github.erdos/routing "0.1.0"]
                 [io.github.erdos/require-all "0.1.1"]
                 [org.slf4j/slf4j-log4j12 "1.7.25"]
                 [com.fasterxml.jackson.core/jackson-core "2.9.2"]
                 [com.fasterxml.jackson.core/jackson-databind "2.9.2"]
                 ]
  :main hu.dbx.kompot.main
  :pom-addition [:properties
                 ["maven.compiler.source" "1.8"]
                 ["maven.compiler.target" "1.8"]])
