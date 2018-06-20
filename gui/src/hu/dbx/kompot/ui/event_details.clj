(ns hu.dbx.kompot.ui.event-details
  (:require [mount.core :refer [defstate]]
            [org.httpkit.server :refer [run-server]]
            [hiccup.def :refer [defhtml]]
            [hu.dbx.kompot.web :refer :all]
            [hu.dbx.kompot.common :refer :all]))


(set! *warn-on-reflection* true)

(defhtml details-group [details uuid group]
  (assert (string? uuid))
  (assert (string? group))
  (let [history (seq (.getEventHistory Reporting uuid group))]
    [:section.section
     [:div.card {:style "box-shadow: 0 2px 6px silver"}
      [:header.card-header
       [:p.card-header-title
        [:span "Processing group " [:big [:code [:b group]]]]]]
      [:div.card-content
       [:div
        (if (empty? history)
          [:div.notification.is-warning
           [:p.has-text-centered [:b "Event processing has not been started (yet)!"]]]
          [:table.table.is-narrow.is-fullwidth.is-bordered
           [:thead [:tr [:th {:style "width: 96px"} "Time"] [:th "Client"] [:th "Status"]]]
           [:tbody (for [row (reverse history)
                         :let [msg (get row "message")
                               exc (get row "exception")]]
                     (list
                      [:tr {:style (status-style (get row "status"))}
                       [:td (get row "time")]
                       [:td (render-client-id (get row "handler"))]
                       [:td (render-status-tag (get row "status"))]]
                      (when (or msg exc)
                        [:tr [:td [:div]]
                         [:td {:colspan 2}
                          (when (seq exc)
                            [:div
                             [:h6.subtitle "An exception occured:"]
                             [:pre (str exc)]])
                          (when (seq msg)
                            [:div
                             [:h6.subtitle "Message:"]
                             [:pre (str msg)]])]])))]])]
       [:div]]]]))

(defhtml app-event [uuid]
  (wrap-html
   [:div
    [:div.has-text-centered {:style "padding: 1em 0"}
     [:a.button {:href "/events"} "Back to events"]]
    [:h1.title.has-text-centered "Async event history"]
    (let [details (into {} (.getEventDetails Reporting uuid))]
      [:div
       [:div.columns
        [:div.column
         [:h2.subtitle.has-text-centered "Details"]
         [:table.table.is-narrow.is-bordered.is-fullwidth
          [:tbody
           [:tr
            [:td [:p.has-text-right "Event code: "]]
            [:td (render-event-name (details "code"))]]
           [:tr
            [:td [:p.has-text-right "Dispatched at: "]]
            [:td [:p [:i (details "firstSent")]]]]
           [:tr
            [:td [:p.has-text-right "Event identifier: "]]
            [:td (render-client-id (str uuid))]]
           [:tr
            [:td [:p.has-text-right "Sender module id:"]]
            [:td (render-client-id (details "sender"))]]]]]
        [:div.column
         [:h2.subtitle.has-text-centered "Data"]
         [:pre (details "data")]]]
       [:h2.subtitle.has-text-centered "Handler Statuses"]
       [:div (for [group (details "groups")]
               (details-group details uuid group))]
       [:div.x-footer [:br]]])]))

(defmethod handle ["events" :uuid] [req]
  {:body (app-event (-> req :route :args first))})

(defmethod handle ["events" :uuid "resend" :number] [req]
  ;; TODO: issue resend...
  (let [uuid          (-> req :route :args first)
        event-details (.getEventDetails Reporting uuid)
        all-groups    (vec (get event-details "groups"))
        event-code    (nth all-groups (-> req :route :args second Integer/parseInt))]
    (.resend Reporting uuid event-code)
    {:headers {"Location" (str "/events/" (-> req :route :args first))}
     :status  301}))
