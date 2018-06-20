(ns hu.dbx.kompot.demo)

(set! *warn-on-reflection* true)

(defn main-server []
  ;; TODO: task is to catch messages and process some of them (with probability)

  )

(defn main-client []
  ;; TODO: task is to create messages every few seconds

  )


(defn -main [& args]
  (cond
    (= args '("server")) (main-server)
    (= args '("client")) (main-client)
    :default             (throw (ex-info "start with server or client mode!"))))
