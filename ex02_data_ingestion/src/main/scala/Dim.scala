/** Définitions des tables de dimensions pour le schéma en étoile (star schema).
  *
  * Ce fichier contient les case classes représentant les tables de dimensions
  * du data warehouse NYC Taxi. Ces dimensions permettent d'enrichir les faits
  * avec des informations descriptives et contextuelles.
  */

/** Dimension des fournisseurs de technologie de taxi.
  *
  * @param vendor_id
  *   Identifiant unique du fournisseur
  * @param vendor_name
  *   Nom complet du fournisseur (ex: Creative Mobile Technologies, LLC)
  */
case class DimVendor(
    vendor_id: Int,
    vendor_name: String
)

/** Dimension des codes tarifaires.
  *
  * @param rate_code_id
  *   Identifiant unique du code tarifaire
  * @param rate_description
  *   Description du tarif (ex: Standard rate, JFK, Newark)
  */
case class DimRateCode(
    rate_code_id: Int,
    rate_description: String
)

/** Dimension des modes de paiement.
  *
  * @param payment_type_id
  *   Identifiant unique du mode de paiement
  * @param payment_description
  *   Description du mode de paiement (ex: Credit card, Cash)
  */
case class DimPaymentType(
    payment_type_id: Int,
    payment_description: String
)

/** Dimension temporelle extraite des timestamps de pickup et dropoff.
  *
  * Cette dimension permet l'analyse temporelle des courses de taxi.
  *
  * @param date_key
  *   Timestamp complet servant de clé primaire
  * @param hour
  *   Heure de la journée (0-23)
  * @param day
  *   Jour du mois (1-31)
  * @param month
  *   Mois de l'année (1-12)
  * @param year
  *   Année
  * @param weekday
  *   Jour de la semaine en texte (ex: Monday, Tuesday)
  */
case class DimDate(
    date_key: java.sql.Timestamp,
    hour: Int,
    day: Int,
    month: Int,
    year: Int,
    weekday: String
)

/** Dimension géographique des zones de taxi de NYC.
  *
  * @param location_id
  *   Identifiant unique de la zone (TLC Taxi Zone)
  * @param borough
  *   Arrondissement de NYC (ex: Manhattan, Brooklyn)
  * @param zone
  *   Nom de la zone spécifique
  * @param service_zone
  *   Type de zone de service
  */
case class DimLocation(
    location_id: Int,
    borough: String,
    zone: String,
    service_zone: String
)
