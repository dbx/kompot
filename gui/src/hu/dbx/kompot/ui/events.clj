(ns hu.dbx.kompot.ui.events
  (:require [mount.core :refer [defstate]]
            [org.httpkit.server :refer [run-server]]
            [hiccup.def :refer [defhtml]]
            [hu.dbx.kompot.web :as web :refer :all]
            [hu.dbx.kompot.common :refer :all]
            [erdos.routing :refer [defreq]]))

(set! *warn-on-reflection* true)

(defhtml app-main []
    (wrap-html
   [:div ;.is-fluid
    (render-menu)
    [:div {:style "padding: 2em 0;"}
     [:div.tabs.is-toggle.is-centered.is-small
      [:ul
       (for [g (.listAllEventGroups Reporting)]
         [:li [:a {:href (str "/events/type/" g)} [:span (str g)]]])]]]
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
                                ))
           :let [uuid (-> g .getEventData .getUuid str)]]
       [:tr
        [:td [:a {:href (str "/events/uuid/" uuid)}
              [:kbd uuid]]]
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

(defreq GET "/events"
  (fn [req] {:body (app-main) :status 200}))

(defreq GET "/events/type/:event-type"
  (fn [req] {:body (app-event-group (-> req :route-params :event-type))}))
