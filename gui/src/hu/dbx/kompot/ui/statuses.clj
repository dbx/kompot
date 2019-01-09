(ns hu.dbx.kompot.ui.statuses
  (:require [mount.core :refer [defstate]]
            [org.httpkit.server :refer [run-server]]
            [hiccup.def :refer [defhtml]]
            [hu.dbx.kompot.web :as web :refer :all]
            [hu.dbx.kompot.common :refer :all]
            [erdos.routing :refer [defreq]]))

(def ^:private css
  (list "table.Flat td, table.Flat th {border: 1px solid white}"
        "table.Flat th {background: #9fa0a0; color: white; font-weight: bold}"
        "td.Good {color: white; text-align:center; background-color: #3bb75d; font-weight: 600}"
        ))

(defhtml render-checkmark-cell [state]
  (case state
    true [:td {:class "Good"} [:b "✓"]]
    false [:td {:class "Bad"} [:b {:style "color: red"} "✗"]]
    [:td [:i {:style "color:silver"} ""]]))

(defhtml app-main []
  (wrap-html
   [:div ;.is-fluid
    [:style css]
    (render-menu)
    ;; itt kilistazzuk az osszes modult!

    [:table {:class "table is-bordered is-narrow Flat"}
    (for [{:keys [eventGroup
                  messageGroup
                  moduleIdentifier
                  items]}
          (->> (.findGlobalStatuses Communication)
               (map bean)
               (sort-by :eventGroup ))]
      [:tbody
       [:tr
        [:th {:colspan 3} [:p [:b (str eventGroup)] " modul"]]]
       (for [{:keys [ok statusMessage errorMessage description]}
             (map bean items)]
         [:tr
          [:td {:style "text-align:right"}
           [:p(str description)]]
          [:td {:style "text-align: center; background: #ccd"}
           [:p (or statusMessage errorMessage)]]
          (render-checkmark-cell ok)])])]
    [:div]
    [:div]]))

(defreq GET "/statuses"
  (fn [req] {:body (app-main) :status 200}))
