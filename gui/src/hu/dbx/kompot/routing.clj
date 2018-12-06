(ns hu.dbx.kompot.routing)

(defonce +default-routes+ {})

(def request-methods '#{GET POST PUT DELETE})

(defmacro defreq
  ([method url return-val]
   `(defreq hu.dbx.kompot.routing/+default-routes+ ~method ~url ~return-val))
  ([routing method url return-val]
   (assert (symbol? routing))
   (assert (symbol? method))
   (assert (string? url))
   (assert (.startsWith (str url) "/"))
   (if ('#{* ANY} method)
     (cons 'do (for [m request-methods] `(defreq ~routing ~m ~url ~return-val)))
     (let [method (keyword (.toLowerCase (name method)))
          r (for [itm (.split (str url) "/"), :when (seq itm)]
              (if (.startsWith (str itm) ":")
                (keyword (.substring (str itm) 1))
                (str itm)))
           assoc-path (map #(if (keyword? %) :* %) r)
           ks          (filter keyword? r)
           handler*    (gensym "handler")
           manual-meta (meta return-val)]
       `(let [~handler* ~return-val]
          (alter-var-root (var ~routing) assoc-in [~method ~@assoc-path :end]
                          {:fn ~(if manual-meta
                                  `(with-meta ~handler* '~manual-meta)
                                  handler*)
                           :ks ~(vec ks)}))))))

(defn- get-handler-step [url routing-map params]
  (if-let [[u & url :as url-rest] (seq url)]
    (or (when (contains? routing-map u)
          (get-handler-step url (get routing-map u) params))
        (when-let [any-route (:* routing-map)]
          (or (get-handler-step url any-route (conj params u))
              (get-handler-step nil any-route (conj params (clojure.string/join "/" url-rest))))))
    (when-let [end (:end routing-map)] {:handler (:fn end) :route-params (zipmap (:ks end) params)})))


(defn get-handler
  ([request] (get-handler +default-routes+ request))
  ([routing-map {:keys [request-method uri]}]
   (assert (map? routing-map))
   (let [route-parts (remove empty? (.split (str uri) "/"))]
     (get-handler-step route-parts (get routing-map request-method) []))))

(defn available-request-methods
  "Returns a set of available request methods"
  ([uri] (available-request-methods +default-routes+ uri))
  ([routing-map uri]
   (let [opts (filter #(get-handler routing-map {:request-method (keyword (.toLowerCase (name %))) :uri uri}) request-methods)]
     (set opts))))

(def ^:private page-404 {:status 404 :body "Route Not Found"})

(defn- page-options [uri]
  (let [methods (available-request-methods uri)]
    {:status 200
     :headers {"accept" (clojure.string/join ", " methods)}}))

(defn handle-routes
  ([req]
   (if-let [{:keys [handler route-params]} (get-handler req)]
     (handler (assoc req :route-params route-params))
     (if (= :options (:request-method req))
       (page-options (:uri req))
       page-404)))
  ([req success error]
   (if-let [{:keys [handler route-params]} (get-handler req)]
     (handler (assoc req :route-params route-params) success error)
     (if (= :options (:request-method req))
       (success (page-options (:uri req)))
       (success page-404)))))
