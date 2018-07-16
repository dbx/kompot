(ns hu.dbx.kompot.common
  (:require [mount.core :refer [defstate]]))

(set! *warn-on-reflection* true)

(defstate redis-uri :start (new java.net.URI "redis://localhost:16379/1"))

(defstate kompot-prefix :start "moby")

(defstate key-naming :start (hu.dbx.kompot.impl.DefaultKeyNaming/ofPrefix (str kompot-prefix)))

(defstate Reporting :start
  (hu.dbx.kompot.report.Reporting/ofRedisConnectionUri redis-uri key-naming))
