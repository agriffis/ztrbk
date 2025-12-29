#!/usr/bin/env bb
;;
;; ztrbk - ZFS snapshot manager in the spirit of btrbk
;;
;; Written in 2025 by Aron Griffis <aron@arongriffis.com>
;;
;; To the extent possible under law, the author(s) have dedicated all copyright
;; and related and neighboring rights to this software to the public domain
;; worldwide. This software is distributed without any warranty.
;;
;; CC0 Public Domain Dedication at
;; http://creativecommons.org/publicdomain/zero/1.0/
;;======================================================================

(ns ztrbk
  (:require [babashka.process :as p]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]))

;; === Configuration ===

(def default-config-path "/etc/ztrbk.edn")

(defn load-config
  [path]
  (edn/read-string (slurp path)))

(def ^:dynamic *dry-run* false)
(def ^:dynamic *safe* false)

;; === ZFS Operations ===

(def _snapshots (atom nil))

(defn fetch-snapshots
  [dataset]
  (let [proc (p/shell {:out :string} "zfs list -H -t snapshot -o name -r" dataset)]
    (->> (str/split-lines (:out proc))
         (remove str/blank?)
         set)))

(defn list-snapshots
  [dataset]
  (or (get @_snapshots dataset)
      (let [fetched (fetch-snapshots dataset)]
        (swap! _snapshots assoc dataset fetched)
        fetched)))

(defn snapshot-dataset
  [snapshot-name]
  (first (str/split snapshot-name #"@")))

(defn snapshot-rest
  [snapshot-name]
  (second (str/split snapshot-name #"@")))

(defn snapshot-exists?
  [snapshot-name]
  (let [snaps (list-snapshots (snapshot-dataset snapshot-name))]
    (boolean (snaps snapshot-name))))

(defn create-snapshot
  "Create snapshot if it does not already exist"
  [dataset prefix timestamp recursive]
  (let [snapshot-name (str dataset "@" prefix timestamp)]
    (when (not (snapshot-exists? snapshot-name))
      (let [cmd (if recursive
                  ["zfs snapshot -r" snapshot-name]
                  ["zfs snapshot" snapshot-name])]
        (apply println (if *dry-run* "[DRY RUN]" "[RUN]") cmd)
        (when-not *dry-run*
          (apply p/shell cmd)))
      (swap! _snapshots update dataset conj snapshot-name)
      snapshot-name)))

(defn destroy-snapshot
  "Destroy snapshot if it exists"
  [snapshot-name]
  (when (snapshot-exists? snapshot-name)
    (let [cmd ["zfs destroy" snapshot-name]]
      (apply println (if (or *dry-run* *safe*) "[DRY RUN]" "[RUN]") cmd)
      (when-not (or *dry-run* *safe*)
        (apply p/shell cmd)))
    (let [dataset (snapshot-dataset snapshot-name)]
      (swap! _snapshots update dataset disj snapshot-name))))

(defn send-snapshot
  "Send snapshot to target dataset, optionally incrementally, if it does not
  already exist"
  ([snapshot-name target-dataset]
   (send-snapshot snapshot-name target-dataset nil))
  ([snapshot-name target-dataset incremental-base]
   (let [target-snapshot (str target-dataset "@" (snapshot-rest snapshot-name))]
     (when-not (snapshot-exists? target-snapshot)
       (let [send-cmd (if incremental-base
                        ["zfs send -w -i" incremental-base snapshot-name]
                        ["zfs send -w" snapshot-name])
             recv-cmd ["zfs receive -e" target-dataset]
             _ (apply println (if *dry-run* "[DRY RUN]" "[RUN]") (concat send-cmd ["|"] recv-cmd))
             proc (when-not *dry-run*
                    (p/pipeline (as-> (apply p/process {:err :inherit} send-cmd) prev
                                  (apply p/shell prev recv-cmd))))]
         (swap! _snapshots update target-dataset conj target-snapshot)
         proc)))))

;; === Timestamp Parsing ===

(def ^:dynamic *now* nil)

(def ^:dynamic *timestamp-format-str* "yyyy-MM-dd_HH-mm")

(def timestamp-formatter
  (memoize (fn [format-str]
             (java.time.format.DateTimeFormatter/ofPattern format-str))))

(defn get-timestamp
  []
  (-> (or *now* (java.time.LocalDateTime/now))
      (.format (timestamp-formatter *timestamp-format-str*))))

(defn parse-timestamp
  "Attempt to parse timestamp string, returning nil on failure"
  [ts-str]
  (try
    (java.time.LocalDateTime/parse ts-str (timestamp-formatter *timestamp-format-str*))
    (catch Exception _
      nil)))

(defn parse-snapshot-timestamp
  "Attempt to parse timestamp from snapshot name with given prefix, returning
  nil for no match"
  [snapshot-name prefix]
  (when-let [snap-part (snapshot-rest snapshot-name)]
    (when (str/starts-with? snap-part prefix)
      (let [ts-str (subs snap-part (count prefix))]
        (parse-timestamp ts-str)))))

;; === Retention Policy ===

(defn time-ago
  ([duration-amount duration-unit]
   (time-ago duration-amount duration-unit (or *now* java.time.LocalDateTime/now)))
  ([duration-amount duration-unit now]
   (case duration-unit
     :hours (.minusHours now duration-amount)
     :days (.minusDays now duration-amount)
     :weeks (.minusWeeks now duration-amount)
     :months (.minusMonths now duration-amount)
     :years (.minusYears now duration-amount))))

(def units [:hours :days :weeks :months :years])

(defn is-valid-unit?
  [unit-kw]
  (some #{unit-kw} units))

(defn normalize-preserve-min
  "Normalize preserve-min to a standard format"
  [spec]
  (cond
    (= spec :all) {:type :all}
    (= spec :latest) {:type :latest}
    (= spec :no) {:type :no}
    (nil? spec) {:type :all}
    (map? spec)
      (do
        (when (not= 1 (count spec))
          (throw (ex-info "Invalid preserve-min count" {:spec spec})))
        (let [[unit amount] (first spec)]
          (when (not (is-valid-unit? unit))
            (throw (ex-info "Invalid preserve-min unit" {:spec spec})))
          {:type :duration :unit unit :amount amount}))
    :else (throw (ex-info "Invalid preserve-min spec" {:spec spec}))))

(defn normalize-preserve
  "Normalize preserve spec to standard format"
  [spec]
  (cond
    (= spec :no) nil
    (nil? spec) nil
    (map? spec) spec
    :else (throw (ex-info "Invalid preserve spec" {:spec spec}))))

(defn format-preserve-min
  "Format preserve-min spec for display"
  [spec]
  (cond
    (= spec :all) "all"
    (= spec :latest) "latest"
    (= spec :no) "no"
    (nil? spec) "all"
    (map? spec) (let [[unit amount] (first spec)]
                  (str amount " " (name unit)))
    :else (str spec)))

(defn format-preserve
  "Format preserve spec for display"
  [spec]
  (cond
    (= spec :no) "no"
    (nil? spec) "no"
    (map? spec) (->> units
                     (keep (fn [unit]
                             (when-let [amount (get spec unit)]
                               (str amount " " (name unit)))))
                     (str/join ", "))
    :else (str spec)))

(defn expired?
  "Check if snapshot falls within amount units"
  [snapshot-ts amount unit]
  (let [cutoff-time (time-ago amount unit)]
    (.isAfter cutoff-time snapshot-ts)))

(defn normalize-day-of-week
  "Normalize day-of-week to integer 1-7 (Monday=1, Sunday=7)"
  [spec]
  (cond
    (int? spec) (cond
                  (= spec 0) 7  ; 0 -> Sunday
                  (= spec 7) 7  ; 7 -> Sunday
                  (<= 1 spec 6) spec
                  :else (throw (ex-info "Invalid day-of-week number" {:spec spec})))
    (keyword? spec) (case spec
                      :monday 1
                      :tuesday 2
                      :wednesday 3
                      :thursday 4
                      :friday 5
                      :saturday 6
                      :sunday 7
                      (throw (ex-info "Invalid day-of-week keyword" {:spec spec})))
    :else (throw (ex-info "Invalid day-of-week spec" {:spec spec}))))

(defn normalize-month-of-year
  "Normalize month-of-year to integer 1-12"
  [spec]
  (cond
    (int? spec) (if (<= 1 spec 12)
                  spec
                  (throw (ex-info "Invalid month-of-year number" {:spec spec})))
    (keyword? spec) (case spec
                      :january 1
                      :february 2
                      :march 3
                      :april 4
                      :may 5
                      :june 6
                      :july 7
                      :august 8
                      :september 9
                      :october 10
                      :november 11
                      :december 12
                      (throw (ex-info "Invalid month-of-year keyword" {:spec spec})))
    :else (throw (ex-info "Invalid month-of-year spec" {:spec spec}))))

(defn get-period-key
  "Get a key for grouping snapshots by period"
  [timestamp unit hour-of-day day-of-week week-of-month month-of-year]
  (let [;; Adjust timestamp so day starts at hour-of-day (for all units except :hours)
        adjusted (if (and (not= unit :hours)
                          (< (.getHour timestamp) hour-of-day))
                   (.minusDays timestamp 1)
                   timestamp)
        ;; Further adjust so month starts at week-of-month (for :months and :years)
        adjusted (if (and (or (= unit :months) (= unit :years))
                          (< (int (Math/ceil (/ (.getDayOfMonth adjusted) 7.0))) week-of-month))
                   (.minusMonths adjusted 1)
                   adjusted)
        ;; Further adjust so year starts at month-of-year (for :years only)
        adjusted (if (and (= unit :years)
                          (< (.getMonthValue adjusted) month-of-year))
                   (.minusYears adjusted 1)
                   adjusted)]
    (case unit
      :hours [(.getYear timestamp)
              (.getMonthValue timestamp)
              (.getDayOfMonth timestamp)
              (.getHour timestamp)]
      :days [(.getYear adjusted)
             (.getMonthValue adjusted)
             (.getDayOfMonth adjusted)]
      :weeks (let [ts-day-of-week (.getValue (.getDayOfWeek adjusted))
                   ;; Calculate days to subtract to get to the target day
                   days-back (mod (+ (- ts-day-of-week day-of-week) 7) 7)
                   week-start (.minusDays adjusted days-back)]
               [(.getYear week-start)
                (.getMonthValue week-start)
                (.getDayOfMonth week-start)])
      :months (let [day-of-month (.getDayOfMonth adjusted)
                    week (int (Math/ceil (/ day-of-month 7.0)))]
                [(.getYear adjusted)
                 (.getMonthValue adjusted)
                 week])
      :years [(.getYear adjusted)])))

(defn apply-retention
  "Apply btrbk-style retention policy"
  [snapshots prefix preserve-min-spec preserve-spec
   hour-of-day day-of-week week-of-month month-of-year]
  (let [;; Parse snapshot timestamps
        snaps-with-ts (keep (fn [snap]
                              (when-let [ts (parse-snapshot-timestamp snap prefix)]
                                {:snapshot snap :timestamp ts}))
                            snapshots)

        ;; Sort by timestamp (newest first)
        snaps-with-ts (reverse (sort-by :timestamp snaps-with-ts))

        ;; First, check preserve-min
        preserved-by-min (case (:type preserve-min-spec)
                           :all (set (map :snapshot snaps-with-ts))
                           :latest #{(:snapshot (first snaps-with-ts))}
                           :no #{}
                           :duration (set (map :snapshot
                                            (remove #(expired?
                                                       (:timestamp %)
                                                       (:amount preserve-min-spec)
                                                       (:unit preserve-min-spec))
                                              snaps-with-ts))))

        ;; Then apply preserve policy (if any)
        preserved-by-policy
          (if (or (nil? preserve-spec)
                  (= (:type preserve-min-spec) :all))
            #{}
            (let [;; Helper to find snapshots for a period
                  find-for-unit
                    (fn [unit]
                      (let [amount (or (preserve-spec unit) 0)
                            unexpired (if (= :all amount)
                                        snaps-with-ts
                                        (take-while #(not (expired? (:timestamp %)
                                                                    amount
                                                                    unit))
                                                    snaps-with-ts))
                            grouped (group-by #(get-period-key (:timestamp %)
                                                               unit
                                                               hour-of-day
                                                               day-of-week
                                                               week-of-month
                                                               month-of-year)
                                              unexpired)
                            keepers (map (fn [[_ snaps]]
                                           (first (sort-by :timestamp compare snaps)))
                                      grouped)]
                        (map :snapshot keepers)))]
              (into #{} (mapcat find-for-unit units))))

        ;; Combine both sets
        all-preserved (into preserved-by-min preserved-by-policy)
        all-snapshot-names (set (map :snapshot snaps-with-ts))
        to-destroy (remove all-preserved all-snapshot-names)]

    {:keep (vec (sort all-preserved))
     :destroy (vec (sort to-destroy))}))

(defn filter-managed-snapshots
  [snapshots prefix]
  (filter #(parse-snapshot-timestamp % prefix) snapshots))

;; === Dataset Configuration ===

(defn compute-retention-settings
  [config global-defaults preserve-min-key preserve-key]
  (let [preserve-min-raw (or (get config preserve-min-key)
                             (get global-defaults preserve-min-key)
                             :all)
        preserve-raw (or (get config preserve-key)
                         (get global-defaults preserve-key)
                         :no)
        preserve-min (normalize-preserve-min preserve-min-raw)
        preserve (normalize-preserve preserve-raw)
        hour-of-day (or (:preserve-hour-of-day config)
                        (:preserve-hour-of-day global-defaults)
                        0)
        day-of-week (normalize-day-of-week
                      (or (:preserve-day-of-week config)
                          (:preserve-day-of-week global-defaults)
                          :sunday))
        week-of-month (or (:preserve-week-of-month config)
                          (:preserve-week-of-month global-defaults)
                          1)
        month-of-year (normalize-month-of-year
                        (or (:preserve-month-of-year config)
                            (:preserve-month-of-year global-defaults)
                            :january))]
    {:preserve-min-raw preserve-min-raw
     :preserve-raw preserve-raw
     :preserve-min preserve-min
     :preserve preserve
     :hour-of-day hour-of-day
     :day-of-week day-of-week
     :week-of-month week-of-month
     :month-of-year month-of-year}))

(defn process-dataset
  [dataset-config global-defaults]
  (let [{:keys [source prefix]} dataset-config
        prefix (or prefix (:prefix global-defaults) "ztrbk_")
        recursive (get (merge global-defaults dataset-config) :recursive true)
        timestamp (get-timestamp)

        ;; Retention settings
        {:keys [preserve-min-raw preserve-raw preserve-min preserve
                hour-of-day day-of-week week-of-month month-of-year]}
          (compute-retention-settings dataset-config
                                      global-defaults
                                      :snapshot-preserve-min
                                      :snapshot-preserve)

        ;; Create new snapshot
        new-snapshot (create-snapshot source prefix timestamp recursive)

        ;; Get all snapshots
        all-snapshots (list-snapshots source)
        managed (filter-managed-snapshots all-snapshots prefix)

        ;; Apply retention
        {keep-local :keep destroy-local :destroy}
          (apply-retention managed
                           prefix
                           preserve-min
                           preserve
                           hour-of-day
                           day-of-week
                           week-of-month
                           month-of-year)]

    (println "\nDataset:" source)
    (println "  snapshot-preserve-min:" (format-preserve-min preserve-min-raw))
    (println "  snapshot-preserve:" (format-preserve preserve-raw))
    (println "  Keeping" (count keep-local) "local snapshots")
    (println "  Destroying" (count destroy-local) "local snapshots")
    (println)

    ;; Destroy old snapshots
    (doseq [snap destroy-local]
      (destroy-snapshot snap))

    {:source source
     :new-snapshot new-snapshot
     :kept-snapshots keep-local
     :prefix prefix
     :hour-of-day hour-of-day
     :day-of-week day-of-week}))

;; === Target/Remote Management ===

(defn find-common-snapshot
  [local-snapshots target-snapshots]
  (let [local-set (set (map snapshot-rest local-snapshots))
        target-set (set (map snapshot-rest target-snapshots))
        common (filter local-set target-set)]
    (when (seq common)
      (last (sort common)))))

(defn process-target
  [target-config dataset-result global-defaults]
  (let [{:keys [target]} target-config
        {:keys [source new-snapshot kept-snapshots prefix]} dataset-result

        ;; Retention settings for target
        {:keys [preserve-min-raw preserve-raw preserve-min preserve
                hour-of-day day-of-week week-of-month month-of-year]}
          (compute-retention-settings target-config
                                      global-defaults
                                      :target-preserve-min
                                      :target-preserve)

        ;; Get target snapshots
        target-snapshots (list-snapshots target)
        target-managed (filter-managed-snapshots target-snapshots prefix)

        ;; Find incremental base
        common (find-common-snapshot kept-snapshots target-managed)
        incremental-base (when common (str source "@" common))

        ;; Check if new snapshot would be kept after applying retention
        would-keep-new? (when new-snapshot
                          (let [target-snapshot-name (str target "@" (snapshot-rest new-snapshot))
                                target-with-new (conj target-managed target-snapshot-name)
                                {keep-with-new :keep}
                                  (apply-retention target-with-new
                                                   prefix
                                                   preserve-min
                                                   preserve
                                                   hour-of-day
                                                   day-of-week
                                                   week-of-month
                                                   month-of-year)]
                            (contains? (set keep-with-new) target-snapshot-name)))

        ;; Send new snapshot only if it would be kept
        _ (when (and new-snapshot would-keep-new?)
            (try
              (send-snapshot new-snapshot target incremental-base)
              (catch Exception e
                (println "  Error sending snapshot:" (.getMessage e)))))

        ;; Apply retention on target
        {keep-target :keep destroy-target :destroy}
          (apply-retention target-managed
                           prefix
                           preserve-min
                           preserve
                           hour-of-day
                           day-of-week
                           week-of-month
                           month-of-year)]

    (println "\n  Target:" target)
    (println "    target-preserve-min:" (format-preserve-min preserve-min-raw))
    (println "    target-preserve:" (format-preserve preserve-raw))
    (println "    Keeping" (count keep-target) "target snapshots")
    (println "    Destroying" (count destroy-target) "target snapshots")
    (if-not would-keep-new?
      (println "    Skipped send: new snapshot would be immediately removed")
      (when incremental-base
        (println "    Used incremental base:" incremental-base)))
    (println)

    ;; Destroy old target snapshots
    (doseq [snap destroy-target]
      (destroy-snapshot snap))))

;; === Main Processing ===

(defn process-config
  [config]
  (let [{:keys [global datasets]} config
        defaults (or global {})]

    (println "=== ztrbk - ZFS snapshot manager ===\n")

    (doseq [dataset-config datasets]
      (try
        (let [dataset-result (process-dataset dataset-config defaults)
              targets (:targets dataset-config)]

          ;; Process targets
          (doseq [target targets]
            (try
              (process-target target dataset-result defaults)
              (catch Exception e
                (println "  Error processing target:" (.getMessage e))))))

        (catch Exception e
          (println "Error processing dataset:" (.getMessage e)))))))

;; === CLI ===

(defn usage
  []
  (println "Usage: ztrbk [options] <command>")
  (println "")
  (println "Commands:")
  (println "  run        Execute snapshot creation and replication")
  (println "")
  (println "Options:")
  (println "  -h, --help           Display this help message and exit")
  (println "  -c, --config FILE    Use specified config file (default: /etc/ztrbk.edn)")
  (println "  -n, --dry-run        Show what would be done without making changes")
  (println "  -s, --safe           Perform operations except zfs destroy")
  (println "")
  (println "Example config file (EDN format):")
  (println "")
  (println "{:global {:prefix \"ztrbk_\"")
  (println "          :preserve-hour-of-day 0")
  (println "          :preserve-day-of-week :sunday")
  (println "          :preserve-week-of-month 1")
  (println "          :preserve-month-of-year :january")
  (println "          :snapshot-preserve-min :all")
  (println "          :snapshot-preserve :no")
  (println "          :target-preserve-min :all")
  (println "          :target-preserve :no}")
  (println " :datasets")
  (println " [{:source \"tank/data\"")
  (println "   ;; Keep all for 14 days, then tiered")
  (println "   :snapshot-preserve-min {:days 14}")
  (println "   :snapshot-preserve {:days 14 :weeks 8 :months 24}")
  (println "   :targets")
  (println "   [{:target \"backup/data\"")
  (println "     :target-preserve-min {:days 30}")
  (println "     :target-preserve {:days 60 :months :all :years 10}}]}")
  (println "")
  (println "  ;; Simple example: keep only latest")
  (println "  {:source \"tank/temp\"")
  (println "   :snapshot-preserve-min :latest")
  (println "   :snapshot-preserve :no")
  (println "   :targets [{:target \"tank/temp-backup\"")
  (println "              :target-preserve-min :latest")
  (println "              :target-preserve :no}]}]}")
  (println "")
  (println "Global and per-dataset/target configuration:")
  (println "  :prefix                    - Snapshot name prefix (default: \"ztrbk_\")")
  (println "  :recursive                 - Create snapshots recursively (default: true)")
  (println "  :preserve-hour-of-day      - Hour when day starts (0-23, default: 0)")
  (println "  :preserve-day-of-week      - Day for weekly snapshots (:sunday-:saturday, or 0-7, default: :sunday)")
  (println "  :preserve-week-of-month    - Week for monthly snapshots (1-4, default: 1)")
  (println "  :preserve-month-of-year    - Month for yearly snapshots (:january-:december, or 1-12, default: :january)")
  (println "  :snapshot-preserve-min     - Local snapshot retention minimum (default: :all)")
  (println "  :snapshot-preserve         - Local snapshot tiered retention (default: :no)")
  (println "  :target-preserve-min       - Target snapshot retention minimum (default: :all)")
  (println "  :target-preserve           - Target snapshot tiered retention (default: :no)")
  (println "")
  (println "Preserve min values:")
  (println "  :all               - Keep everything forever (default)")
  (println "  :latest            - Keep only newest snapshot")
  (println "  :no                - Don't keep anything (rely on preserve only)")
  (println "  {:hours N}         - Keep all for N hours")
  (println "  {:days N}          - Keep all for N days")
  (println "  {:weeks N}         - Keep all for N weeks")
  (println "  {:months N}        - Keep all for N months")
  (println "  {:years N}         - Keep all for N years")
  (println "")
  (println "Preserve format (map with any combination):")
  (println "  {:hours N}         - Keep N hourly snapshots")
  (println "  {:days N}          - Keep N daily snapshots")
  (println "  {:weeks N}         - Keep N weekly snapshots")
  (println "  {:months N}        - Keep N monthly snapshots")
  (println "  {:years N}         - Keep N yearly snapshots")
  (println "  Use :all for unlimited (e.g., {:months :all})")
  (println "")
  (println "How it works:")
  (println "  1. preserve-min: Keep ALL snapshots within this duration")
  (println "  2. preserve: Keep specific snapshots (hourly/daily/weekly/monthly/yearly)")
  (println "     - Snapshots within preserve-min are already kept")
  (println "     - preserve adds ADDITIONAL snapshots beyond preserve-min"))

(defn -main
  [& args]
  (let [parsed (loop [args args
                      config-path default-config-path
                      command nil
                      dry-run? false
                      safe? false]
                 (if-let [arg (first args)]
                   (case arg
                     ("-h" "--help") (do (usage) (System/exit 0))
                     ("-c" "--config") (recur (drop 2 args) (second args) command dry-run? safe?)
                     ("-n" "--dry-run") (recur (rest args) config-path command true safe?)
                     ("-s" "--safe") (recur (rest args) config-path command dry-run? true)
                     "run" (recur (rest args) config-path arg dry-run? safe?)
                     (do
                       (println "Unknown argument:" arg)
                       (System/exit 1)))
                   {:config-path config-path :command command :dry-run? dry-run? :safe? safe?}))
        {:keys [config-path command dry-run? safe?]} parsed]

    (when-not command
      (println "No command specified.")
      (println "Use -h for help.")
      (System/exit 1))

    (when-not (.exists (io/file config-path))
      (println "Config file not found:" config-path)
      (System/exit 1))

    (binding [*dry-run* dry-run?
              *safe* safe?
              *now* (java.time.LocalDateTime/now)]
      (let [config (load-config config-path)]
        (process-config config)))))

(when (= *file* (System/getProperty "babashka.file"))
  (apply -main *command-line-args*))
