(defproject hu.dbx/kompot-gui "1.0.0-SNAPSHOT"
  :description "Graphical interface for kompot events"
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [mount "0.1.11"]
                 [hiccup "1.0.5"]
                 [http-kit "2.2.0"]
                 [hu.dbx/kompot "0.1.2-SNAPSHOT"]
                 [org.slf4j/slf4j-log4j12 "1.7.25" ]]
  :main hu.dbx.kompot.main
  :pom-addition [:properties
                 ["maven.compiler.source" "1.8"]
                 ["maven.compiler.target" "1.8"]])
