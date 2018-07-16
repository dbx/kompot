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
    [:div {:style "padding: 2em 0;"}
     [:div.tabs.is-toggle.is-centered.is-small
      [:ul
       (for [g (.listAllEventGroups Reporting)]
         [:li [:a {:href (str "/events/" g)}
               [:span (str g)]]])]]]
    [:div]]))

(defhtml app-event-group [event-group-name]
  (wrap-html
   [:div ;.is-fluid
    (render-menu)

    [:table.table.is-fullwidth
     [:thead [:tr
              [:th "Event Identifier"]
              [:th "Event Type"]
              [:th "Sender Identifier"]
              [:th "First Sent"]
              [:th "Status"]]]
     [:tbody]
     (for [g (seq (.queryEvents Reporting
                                (str event-group-name)
                                nil
                                (hu.dbx.kompot.report.Pagination/fromOffsetAndLimit 0 100)
                                ))]
       [:tr
        [:td [:kbd (str (-> g .getEventData .getUuid))]]
        [:td [:code (str (-> g .getEventData .getEventType))]]
        [:td [:kbd (str (-> g .getEventData .getSender))]]
        [:td [:p [:i (str (-> g .getEventData .getFirstSent))]]]
        [:td (-> g .getStatus str render-status-tag)]
        ])]

    #_
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
      (for [row (reverse (seq (.queryEvents Reporting "POLICY")))
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
    [:div]])
  )

(defmethod web/handle ["events"] [req]
  {:body (app-main)})

(defmethod web/handle ["events" "POLICY"] [req]
  {:body (app-event-group "POLICY")})
