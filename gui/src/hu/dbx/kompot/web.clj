(ns hu.dbx.kompot.web
  "Helpers for web servers"
  (:require [hiccup.def :refer [defhtml]]))

(set! *warn-on-reflection* true)

(defn parse-uri [s]
  (let [parts (filter seq (.split s "/"))
        ptf   (fn [s] (cond
                        (re-matches #"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}" s) :uuid
                        (re-matches #"[0-9]+" s) :number
                        :otherwise    s))]
    {:uri  (mapv ptf parts)
     :args (filterv (comp not string? ptf) parts)}))

;; (parse-uri "/events/3")

(defmulti handle (comp :uri :route))


(defn handler [request]
  (handle (assoc request :route (parse-uri (:uri request)))))

(defhtml wrap-html [contents]
  [:html
   [:head
    [:link {:rel :stylesheet
            :href "https://jenil.github.io/bulmaswatch/litera/bulmaswatch.min.css"}]
    [:link {:rel :stylesheet
            :href "https://maxcdn.bootstrapcdn.com/font-awesome/4.7.0/css/font-awesome.min.css"}]]
   [:body
    [:div.container ;.is-fluid
     contents]]])

(defn render-menu []
  [:div {:style "padding: 2em 0;"}
   [:div.tabs.is-toggle.is-centered.is-small
    [:ul
     [:li.is-active [:a [:span "Events"]]]
     [:li [:a [:span "Messages"]]]
     [:li [:a [:span "Broadcasts"]]]
     [:li [:a [:span "Statuses"]]]]]])

(def status-style {"processing" "background:hsl(48, 100%, 67%)"
                   "failed"     "background:hsl(348, 80%, 81%)"})

(defn render-event-name [event-name]
  [:span.tag (str event-name)])

(defn render-client-id [client-id]
  [:small [:small [:u (str client-id)]]])

(defn render-group-name [group-name]
  [:p [:code group-name]])

(defn- icon [icon-name]
  [:span.icon [(keyword (str "i.fa.fa-" (name icon-name)))]])

(defn render-status-tag [status]
  (case (.toLowerCase (str status))
    ("" nil)                  [:span.tag.is-rounded "???"]
    ("done" "processed" "ok") [:span.tag.is-rounded.is-success (icon :check) status]
    ("processing")            [:span.tag.is-rounded.is-warning (icon :exclamation-circle) status]
    ("failed" "fail" "error") [:span.tag.is-rounded.is-danger (icon :exclamation-triangle) status]
    [:span.tag (str status)]))

:OK
