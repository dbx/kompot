(ns hu.dbx.kompot.ui.event-details
  (:import [java.util UUID])
  (:require [mount.core :refer [defstate]]
            [org.httpkit.server :refer [run-server]]
            [hiccup.def :refer [defhtml]]
            [hu.dbx.kompot.web :refer :all]
            [hu.dbx.kompot.common :refer :all]
            [hu.dbx.kompot.util :refer :all]
            [erdos.routing :refer [defreq]]))


(set! *warn-on-reflection* true)

(def processing-not-started
  [:div.notification.is-warning
   [:p.has-text-centered [:b "Event processing has not been started (yet)!"]]])

(defn- render-exception [exc]
  [:div
   [:h6.subtitle "An exception occured:"]
   [:pre (str exc)]])

(defn- render-message [msg]
  [:div
   [:h6.subtitle "Message:"]
   [:pre (str msg)]])

(defn- render-history-item [row]
  (let [msg (get row "message")
        exc (get row "exception")]
    (list
     [:tr {:style (status-style (get row "status"))}
      [:td (get row "time")]
      [:td (render-client-id (get row "handler"))]
      [:td (render-status-tag (get row "status"))]]
     (when (or msg exc)
       [:tr [:td [:div]]
        [:td {:colspan 2}
         (when (seq exc)
           (render-exception exc))
         (when (seq msg)
           (render-message msg))]]))))

(defhtml details-group [details uuid group]
  (assert (uuid? uuid))
  (assert (string? group))
  (let [evt (.querySingleEvent Reporting (str group) uuid)]
    (if (.isPresent evt)
      (let [evt (.get evt)
            history (vec (.getHistory evt))]
        [:section.section
         [:div.card {:style "box-shadow: 0 2px 6px silver"}
          [:header.card-header
           [:p.card-header-title
            [:span "Processing group " [:big [:code [:b group]]]]]]
          [:div.card-content
           [:div
            (if (empty? history)
              processing-not-started
              [:table.table.is-narrow.is-fullwidth.is-bordered
               [:thead [:tr [:th {:style "width: 96px"} "Time"] [:th "Client"] [:th "Status"]]]
               [:tbody (for [row (reverse history)] (render-history-item row))]])]
           [:div]]]])
      [:i "Details not found for event processor " [:b group]])))

(defhtml app-event [uuid]
  (wrap-html
   [:div
    [:div.has-text-centered {:style "padding: 1em 0"}
     [:a.button {:href "/events"} "Back to events"]]
    [:h1.title.has-text-centered "Async event history"]
    (let [{:keys [eventType sender firstSent groups data] :as details}
          (bean (.get (.queryEventData Reporting uuid)))]
      [:div
       [:div.columns
        [:div.column
         [:h2.subtitle.has-text-centered "Details"]
         [:table.table.is-narrow.is-bordered.is-fullwidth
          [:tbody
           [:tr
            [:td [:p.has-text-right "Event code: "]]
            [:td (render-event-name eventType)]]
           [:tr
            [:td [:p.has-text-right "Dispatched at: "]]
            [:td [:p [:i firstSent]]]]
           [:tr
            [:td [:p.has-text-right "Event identifier: "]]
            [:td (render-client-id uuid)]]
           [:tr
            [:td [:p.has-text-right "Sender module id:"]]
            [:td (render-client-id sender)]]]]]
        [:div.column
         [:h2.subtitle.has-text-centered "Data"]
         [:pre data]]]
       [:h2.subtitle.has-text-centered "Handler Statuses"]
       [:div (for [group (.split groups ",")]
               (details-group details uuid group))]
       [:div.x-footer [:br]]])]))

(defn url-event [uuid] (str "/events/uuid/" uuid))

(defreq GET "/events/uuid/:uuid"
  (fn [req] {:body (app-event (-> req :route-params :uuid UUID/fromString)) :status 200}))

(defreq POST "/events/uuid/:uuid/resend/:n"
  (fn [req]
    (let [uuid          (-> req :route-params :uuid)
          event-details (.getEventDetails Reporting uuid)
          all-groups    (vec (get event-details "groups"))
          event-code    (nth all-groups (-> req :route-params :n ->int))]
      (.resend Reporting uuid event-code)
      {:headers {"Location" (url-event uuid)}
       :status  301})))
