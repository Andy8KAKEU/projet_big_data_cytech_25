import java.sql.Timestamp

/** Classe représentant le schéma brut des données NYC Yellow Taxi.
  *
  * Cette case class modélise la structure des fichiers Parquet bruts
  * téléchargés depuis l'API NYC Taxi. Elle contient tous les champs du dataset
  * officiel, incluant les nouveaux frais introduits en janvier 2025.
  *
  * @param VendorID
  *   Identifiant du fournisseur de technologie (1=Creative Mobile, 2=Curb,
  *   etc.)
  * @param tpep_pickup_datetime
  *   Date et heure de prise en charge du passager
  * @param tpep_dropoff_datetime
  *   Date et heure de dépose du passager
  * @param passenger_count
  *   Nombre de passagers dans le véhicule
  * @param trip_distance
  *   Distance du trajet en miles
  * @param RatecodeID
  *   Code tarifaire appliqué (1=Standard, 2=JFK, 3=Newark, etc.)
  * @param PULocationID
  *   Identifiant de la zone de prise en charge
  * @param DOLocationID
  *   Identifiant de la zone de dépose
  * @param payment_type
  *   Type de paiement (1=Carte, 2=Espèces, etc.)
  * @param fare_amount
  *   Montant de la course calculé par le compteur
  * @param extra
  *   Frais supplémentaires (heures de pointe, nuit)
  * @param mta_tax
  *   Taxe MTA (Metropolitan Transportation Authority)
  * @param tip_amount
  *   Montant du pourboire (automatique pour paiements par carte)
  * @param tolls_amount
  *   Montant total des péages
  * @param improvement_surcharge
  *   Surcharge d'amélioration (fixe à 1 dollar)
  * @param total_amount
  *   Montant total facturé au passager
  * @param congestion_surcharge
  *   Surcharge de congestion pour Manhattan
  * @param airport_fee
  *   Frais d'aéroport (pour trajets JFK et LaGuardia)
  * @param cbd_congestion_fee
  *   Nouveau frais de congestion CBD (Central Business District) depuis janvier
  *   2025
  */
case class YelloxTrip(
    VendorID: Int,
    tpep_pickup_datetime: Timestamp,
    tpep_dropoff_datetime: Timestamp,
    passenger_count: Long,
    trip_distance: Double,
    RatecodeID: Long,
    PULocationID: Long,
    DOLocationID: Long,
    payment_type: Long,
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
