(ns hu.dbx.kompot.common
  (:require [mount.core :refer [defstate]]))

(set! *warn-on-reflection* true)

(defstate KVStore :start (hu.dbx.kompot.impl.RedisKVStore/buildDefault))
(defstate Naming :start (new hu.dbx.kompot.impl.DefaultKeyNaming "moby"))
(defstate Reporting :start (new hu.dbx.kompot.report.Reporting KVStore Naming))

;; itt csak egy CommunicationEndpoint vegpont kell