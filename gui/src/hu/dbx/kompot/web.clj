(ns hu.dbx.kompot.web
  "Helpers for web servers"
  (:require [hiccup.def :refer [defhtml]]
            [erdos.routing :refer [defreq handle-routes]]))

(set! *warn-on-reflection* true)

(def handler handle-routes)

(defhtml wrap-html [contents]
  [:html
   [:head
    [:meta {:charset "UTF-8"}]
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
     (letfn [(item [url title]
               [:li (when (.endsWith (str (:uri erdos.routing/*request*)) (str url))
                      {:class "is-active"})
                [:a {:href url} [:span title]]])]
       (list
        (item "/events" "Events")
        (item "/messages" "Messages")
        (item "/broadcasts" "Broadcasts")
        (item "/statuses" "Statuses")))]]])

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
    ("")                      [:span.tag.is-rounded "???"]
    ("done" "processed" "ok") [:span.tag.is-rounded.is-success (icon :check) status]
    ("processing")            [:span.tag.is-rounded.is-warning (icon :exclamation-circle) status]
    ("failed" "fail" "error") [:span.tag.is-rounded.is-danger (icon :exclamation-triangle) status]
    [:span.tag (str status)]))

:OK
