/** Table de faits des courses de taxi (fact table du schéma en étoile).
  *
  * Cette case class représente la table centrale du data warehouse contenant
  * les métriques et mesures des courses de taxi, avec des clés étrangères vers
  * les tables de dimensions.
  *
  * @param pickup_datetime_key
  *   Clé étrangère vers dim_date (timestamp de prise en charge)
  * @param dropoff_datetime_key
  *   Clé étrangère vers dim_date (timestamp de dépose)
  * @param vendor_key
  *   Clé étrangère vers dim_vendor
  * @param pu_location_key
  *   Clé étrangère vers dim_location (zone de prise en charge)
  * @param do_location_key
  *   Clé étrangère vers dim_location (zone de dépose)
  * @param ratecode_key
  *   Clé étrangère vers dim_rate_code
  * @param payment_key
  *   Clé étrangère vers dim_payment_type
  * @param passenger_count
  *   Nombre de passagers transportés
  * @param trip_distance
  *   Distance parcourue en miles
  * @param fare_amount
  *   Montant de la course (hors frais)
  * @param extra
  *   Frais supplémentaires (heures de pointe, nuit)
  * @param mta_tax
  *   Taxe MTA
  * @param tip_amount
  *   Montant du pourboire
  * @param tolls_amount
  *   Montant des péages
  * @param improvement_surcharge
  *   Surcharge d'amélioration
  * @param total_amount
  *   Montant total de la course
  * @param congestion_surcharge
  *   Surcharge de congestion
  * @param airport_fee
  *   Frais d'aéroport
  * @param cbd_congestion_fee
  *   Frais de congestion CBD
  */
case class FactTrip(
    pickup_datetime_key: Long,
    dropoff_datetime_key: Long,
    vendor_key: Int,
    pu_location_key: Int,
    do_location_key: Int,
    ratecode_key: Int,
    payment_key: Int,
    passenger_count: Long,
    trip_distance: Double,
    fare_amount: Double,
    extra: Double,
    mta_tax: Double,
    tip_amount: Double,
    tolls_amount: Double,
    improvement_surcharge: Double,
    total_amount: Double,
    congestion_surcharge: Double,
    airport_fee: Double,
    cbd_congestion_fee: Double
)
