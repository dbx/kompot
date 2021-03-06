(ns hu.dbx.kompot.ui.event-history
  (:require [mount.core :refer [defstate]]
            [org.httpkit.server :refer [run-server]]
            [hiccup.def :refer [defhtml]]
            [hu.dbx.kompot.web :as web :refer :all]
            [hu.dbx.kompot.common :refer :all]
            [clojure.pprint :refer [pprint]]))

(set! *warn-on-reflection* true)

(defhtml view [event-details group-history]
  (wrap-html
   [:div.content
    [:div.has-text-centered {:style "padding: 1em 0"}
     [:a.button {:href "/events"} "Back to events"]]
    [:h1.title.has-text-centered "Async event history"]
    [:pre (with-out-str (pprint event-details))]
    [:pre (with-out-str (pprint group-history))]
    (let []
      [:div
       [:div.columns
        [:div.column.content
         [:a.button {:href (str "/events/" (get event-details "uuid"))} "Back to event"]
         [:h2.subtitle.has-text-centered "Details"]
         #_[:table.table.is-narrow.is-bordered
          [:tbody
           [:tr
            [:td [:p "Event identifier: "]]
            [:td [:p [:code (str uuid)]]]]
           [:tr
            [:td [:p "Event code: "]]
            [:td [:p [:code (details "code")]]]]
           [:tr
            [:td [:p "Dispatched at: "]]
            [:td [:p [:i (details "firstSent")]]]]
           [:tr
            [:td [:p "Sender module id:"]]
            [:td [:p [:code (details "sender")]]]]]]]
        #_[:div.column
         [:h2.subtitle.has-text-centered "Data"]
         [:pre (details "data")]]]
       [:h2.subtitle.has-text-centered "Handler Statuses"]
       [:table.table.is-fullwidth.is-narrow
        [:thead
         [:tr
          [:th [:p "Group"]]
          [:th "Last change time"]
          [:th "Last change status"]]]
        #_[:tbody
         (for [group (details "groups")]
           (details-group uuid group))]]])]))

;; TODO: send status code too.
(defmethod handle ["events" :uuid :number] [req]
  (let [uuid          (-> req :route :args first)
        event-details (.getEventDetails Reporting uuid)
        all-groups    (vec (get event-details "groups"))
        event-code    (nth all-groups (-> req :route :args second Integer/parseInt))
        group-history (.getEventHistory Reporting uuid event-code)]
    {:body (view event-details group-history)}))
