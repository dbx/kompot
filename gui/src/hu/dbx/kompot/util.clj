(ns hu.dbx.kompot.util)

(defn ->int [s] (some-> s str Integer/parseInt))

(defn uuid? [x] (instance? java.util.UUID x))
