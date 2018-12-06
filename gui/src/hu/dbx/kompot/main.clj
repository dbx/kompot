(ns hu.dbx.kompot.main
  (:require [mount.core :refer [defstate]]
            [org.httpkit.server :refer [run-server]]
            [hiccup.def :refer [defhtml]]
            [hu.dbx.kompot.web :as web]
            [hu.dbx.kompot.ui.events]
            [hu.dbx.kompot.ui.event-details]
            [hu.dbx.kompot.ui.event-history]
            [hu.dbx.kompot.common :refer :all]
            [hu.dbx.kompot.routing :refer [defreq]]))

(set! *warn-on-reflection* true)

;; TODO: implement batch_framework with it.

(defstate web-port :start 8080) ;; TODO: get it from config.

(defreq GET "/" (fn [req] {:headers {"Location" "/events"} :status 301}))

(defstate WebServer
  :start (run-server #'web/handler {:port web-port})
  :stop (WebServer))

(defn -main [& args]
  (println "Starting up state...")
  (mount.core/start)
  (do (println "State started, press enter to quit.") (read-line))
  (mount.core/stop)
  (println "State stopped. Quitting.")
  (shutdown-agents))
