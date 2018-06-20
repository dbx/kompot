(ns hu.dbx.kompot.ui.events
  (:require [mount.core :refer [defstate]]
            [org.httpkit.server :refer [run-server]]
            [hiccup.def :refer [defhtml]]
            [hu.dbx.kompot.web :as web :refer :all]
            [hu.dbx.kompot.common :refer :all]))

(set! *warn-on-reflection* true)

(defhtml app-main []
  (wrap-html
   [:div ;.is-fluid
    (render-menu)
    #_[:div
       [:h1.title.has-text-centered "Async events."]
       ]

    #_[:h2.title.has-text-centered "Event phases" ]

    [:table.table.is-fullwidth
     [:thead
      [:tr
       [:th "Creation time"]
       [:th "Event name"]
       [:th "Creation identifier"]
       [:th "Group"]
       [:th "Handler id"]
       [:th "Status change time"]
       [:th "Status"]]]
     [:tbody
      (for [row (reverse (seq (.allEvents Reporting)))
            :let [uuid (get row "uuid")
                  status (get row "status")]]
        [:tr {:style (status-style status)}
         [:td [:small [:i (get row "time")]]]
         [:td (render-event-name (get row "code"))]
         [:td [:small [:small [:a {:href (str "/events/" uuid)} [:u (str uuid)]]]]]
         [:td (render-group-name (get row "group"))]
         [:td (render-client-id (get row "handler"))]
         [:td [:small [:i (some-> row (get "handleTime") (java.util.Date.))]]]
         [:td (render-status-tag status)]
         ])]]
    [:div]]))

(defmethod web/handle ["events"] [req]
  {:body (app-main)})
