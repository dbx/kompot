(ns hu.dbx.kompot.common
  (:require [mount.core :refer [defstate]]))

(set! *warn-on-reflection* true)

(defstate ^java.net.URI redis-uri :start
  #_
  (if-let [uri (System/getenv "KOMPOT_REDIS_URI")]
    (new java.net.URI (str uri) #_"redis://localhost:26379/1")
    (throw (ex-info "Env var KOMPOT_REDIS_URI is not set!")))
  (new java.net.URI "redis://localhost:26379/1"))

(defstate kompot-prefix :start "moby")

(defstate key-naming :start (hu.dbx.kompot.impl.DefaultKeyNaming/ofPrefix (str kompot-prefix)))

(defstate Reporting :start
  (hu.dbx.kompot.report.Reporting/ofRedisConnectionUri redis-uri key-naming))

(defstate ^hu.dbx.kompot.CommunicationEndpoint Communication
  :start
  (doto
      (hu.dbx.kompot.CommunicationEndpoint/ofRedisConnectionUri
       redis-uri
       (hu.dbx.kompot.producer.EventGroupProvider/empty)
       (hu.dbx.kompot.impl.DefaultConsumerIdentity/groupGroup "REPORTER-UI"))
    (.start))
  :stop (.stop Communication))
