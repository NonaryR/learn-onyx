(ns workshop.challenge-2-3
  (:require [workshop.workshop-utils :as u]))

;;; Workflows ;;;

(def workflow
  [[:read-segments :identity]
   [:identity :write-segments]])

;;; Catalogs ;;;

(defn build-catalog
  ([] (build-catalog 5 50))
  ([batch-size batch-timeout]
     [{:onyx/name :read-segments
       :onyx/plugin :onyx.plugin.core-async/input
       :onyx/type :input
       :onyx/medium :core.async
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/max-peers 1
       :onyx/doc "Reads segments from a core.async channel"}

      ;; <<< BEGIN FILL ME IN >>>

      {:onyx/name :identity
       :onyx/fn :clojure.core/identity
       :onyx/type :function
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/doc "identity"}

      ;; <<< END FILL ME IN >>>

      {:onyx/name :write-segments
       :onyx/plugin :onyx.plugin.core-async/output
       :onyx/type :output
       :onyx/medium :core.async
       :onyx/batch-size batch-size
       :onyx/batch-timeout batch-timeout
       :onyx/max-peers 1
       :onyx/doc "Writes segments to a core.async channel"}]))

;;; Functions ;;;


;;; Lifecycles ;;;

;; Serialize print statements to avoid garbled stdout.
(def printer (agent nil))

(defn inject-writer-ch [event lifecycle]
  {:core.async/chan (u/get-output-channel (:core.async/id lifecycle))})

(defn echo-segments [event lifecycle]
  (send printer
        (fn [_]
          (doseq [segment (:onyx.core/batch event)]
            (println (format "Peer %s saw segment %s"
                             (:onyx.core/id event)
                             segment)))))
  {})

;; event
#_(:onyx.core/outbox-ch :onyx.core/batch :onyx.core/log-prefix :onyx.core/workflow :assign-watermark-fn :onyx.core/job-config :onyx.core/output-plugin :onyx.core/task-kill-flag :onyx.core/id :onyx.core/write-batch :onyx.core/task-map :onyx.core/flow-conditions :onyx.core/storage :onyx.core/task-information :onyx.core/replica-atom :onyx.core/catalog :onyx.core/input-plugin :onyx.core/log :onyx.core/metadata :grouping-fn :onyx.core/group-ch :onyx.core/lifecycle-id :onyx.core/monitoring :onyx.core/job-name :onyx.core/fn :onyx.core/resume-point :onyx.core/tenancy-id :onyx.core/transformed :onyx.core/peer-opts :onyx.core/job-id :onyx.core/triggered :onyx.core/windows :onyx.core/task :compiled-non-ex-routes :compiled-norm-fcs :onyx.core/slot-id :onyx.core/task-id :onyx.core/since-barrier-count :onyx.core/lifecycles :onyx.core/params :egress-tasks :compiled-ex-fcs :onyx.core/serialized-task :onyx.core/kill-flag :onyx.core/triggers)

(def writer-lifecycle
  {:lifecycle/before-task-start inject-writer-ch})

(def identity-lifecycle
  {:lifecycle/after-batch echo-segments})

(defn build-lifecycles []
  [{:lifecycle/task :read-segments
    :lifecycle/calls :workshop.workshop-utils/in-calls
    :core.async/id (java.util.UUID/randomUUID)
    :onyx/doc "Injects the core.async reader channel"}

   {:lifecycle/task :read-segments
    :lifecycle/calls :onyx.plugin.core-async/reader-calls
    :onyx/doc "core.async plugin base lifecycle"}

   {:lifecycle/task :write-segments
    :lifecycle/calls :workshop.challenge-2-3/writer-lifecycle
    :core.async/id (java.util.UUID/randomUUID)
    :onyx/doc "Injects the core.async writer channel"}

   {:lifecycle/task :write-segments
    :lifecycle/calls :onyx.plugin.core-async/writer-calls
    :onyx/doc "core.async plugin base lifecycle"}

   {:lifecycle/task :identity
    :lifecycle/calls :workshop.challenge-2-3/identity-lifecycle
    :onyx/doc "Lifecycle for logging segments"}])
