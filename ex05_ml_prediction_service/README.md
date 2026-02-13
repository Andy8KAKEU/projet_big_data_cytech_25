# Service de Prédiction ML - Pourboires de Taxi

## Rappel

L'utilisation de python natif est strictement interdit. Vous devez utiliser les environnements virtuelles gérés par uv.

## Objectif

Nous utilisons la librairie **SparkML** pour développer un modèle de prédiction du pourboire d'une course de taxi.

## Contexte

Nous avons un service de course de taxi qui souhaite prédire si un client va donner un pourboire ou non. Ce modèle de classification binaire utilise les données **Parquet nettoyées depuis MinIO** (bucket `nyc-cleaned`) pour entraîner un algorithme de **Random Forest**.

---

## 🏗️ Structure du Projet

```
ex05_ml_prediction_service/
├── src/
│   ├── main/
│   │   ├── scala/
│   │   │   ├── Main.scala                 # Point d'entrée principal
│   │   │   ├── DataPreparation.scala      # Préparation et feature engineering
│   │   │   ├── TipPredictionModel.scala   # Définition du modèle ML
│   │   │   └── ModelEvaluation.scala      # Évaluation et métriques
│   │   └── resources/
│   │       └── application.conf            # Configuration du modèle
└── README.md
```

---

## 🎯 Variable Cible

**Label**: `1` si `tip_amount > 0`, `0` sinon

Le modèle prédit si le client donnera un pourboire (classe 1) ou non (classe 0).

---

## 📊 Source de Données

Les données sont lues directement depuis **MinIO** au format **Parquet** :
- **Bucket** : `nyc-cleaned` (configurable via `CLEANED_BUCKET_NAME`)
- **Fichier** : `yellow_tripdata_2025-08_cleaned.parquet` (configurable via `FILE_NAME_BUCKET_FIRST_DEPOSIT`)

Ce fichier Parquet est produit par le pipeline ETL de **ex02** (Bronze → Silver).

---

## 📊 Features Utilisées (14 features)

### Features directes du Parquet

| Feature                 | Description                    | Type      |
|-------------------------|--------------------------------|-----------|
| `fare_amount`           | Montant de la course           | Numérique |
| `trip_distance`         | Distance du trajet (miles)     | Numérique |
| `passenger_count`       | Nombre de passagers            | Numérique |
| `rate_code_id`          | Type de tarif appliqué         | Catégorie |
| `payment_type_id`       | Type de paiement               | Catégorie |
| `extra`                 | Frais supplémentaires          | Numérique |
| `mta_tax`               | Taxe MTA                       | Numérique |
| `tolls_amount`          | Montant des péages             | Numérique |
| `congestion_surcharge`  | Surcharge de congestion        | Numérique |
| `airport_fee`           | Frais d'aéroport               | Numérique |
| `cbd_congestion_fee`    | Frais de congestion CBD        | Numérique |
| `improvement_surcharge` | Surcharge d'amélioration       | Numérique |

### Features dérivées (Feature Engineering)

| Feature                 | Description                             | Source                  |
|-------------------------|-----------------------------------------|-------------------------|
| `pickup_hour`           | Heure de prise en charge                | `hour(pickup_datetime)` |
| `trip_duration_minutes` | Durée du trajet en minutes              | `dropoff - pickup`      |

**Prétraitement**:
- Normalisation avec `StandardScaler` (moyenne = 0, écart-type = 1)
- Remplissage des valeurs manquantes par 0
- Filtrage des données invalides (montants ou distances négatifs, durées aberrantes > 300 min)

---

## 🤖 Algorithme de Machine Learning

**Random Forest Classifier**

### Hyperparamètres
- **Nombre d'arbres**: 100
- **Profondeur maximale**: 10
- **Instances min par nœud**: 5
- **Stratégie de features**: auto
- **Impureté**: Gini
- **Seed**: 42 (pour reproductibilité)

### Pipeline ML
1. **Feature Engineering**: Extraction de `pickup_hour` et `trip_duration_minutes`
2. **VectorAssembler**: Assemblage des 14 features
3. **StandardScaler**: Normalisation des features
4. **RandomForestClassifier**: Classification binaire

---

## 🚀 Exécution du Modèle

### Prérequis
- MinIO en cours d'exécution avec le bucket `nyc-cleaned` rempli (via ex02)
- Docker services (MinIO) en cours d'exécution
- SBT installé

### Commandes

#### 1. Compiler le projet
```bash
cd /home/cytech/BigData/projet_big_data_cytech_25
sbt ex5/compile
```

#### 2. Exécuter l'entraînement et l'évaluation
```bash
sbt "ex5/run"
```

---

## 📈 Métriques d'Évaluation

Le modèle est évalué avec les métriques suivantes:

### Métriques de Classification
- **Accuracy** (Exactitude): Proportion de prédictions correctes
- **Precision** (Précision): Proportion de vrais positifs parmi les prédictions positives
- **Recall** (Rappel): Proportion de vrais positifs détectés
- **F1 Score**: Moyenne harmonique de la précision et du rappel

### Métriques Binaires
- **AUC-ROC**: Aire sous la courbe ROC
- **AUC-PR**: Aire sous la courbe Precision-Recall

### Matrice de Confusion
```
                    Prédit: Non-Tip    Prédit: Tip
   Réel: Non-Tip        TN                 FP
   Réel: Tip            FN                 TP
```

---

## 🔧 Configuration

Les paramètres du modèle peuvent être modifiés dans `src/main/resources/application.conf`:

```hocon
minio {
  bucketName = "nyc-cleaned"      # Bucket MinIO source
}

model.randomForest {
  numTrees = 100          # Nombre d'arbres
  maxDepth = 10           # Profondeur maximale
  # ... autres paramètres
}
```

---

## 📝 Notes Techniques

### Normalisation des Features
Les features sont normalisées avec `StandardScaler` pour:
- Améliorer la convergence de l'algorithme
- Éviter que les features avec de grandes valeurs dominent
- Rendre les coefficients comparables

### Gestion du Déséquilibre de Classes
Si la distribution des classes est déséquilibrée, considérez:
- Ajuster les poids de classes dans le modèle
- Utiliser des techniques de sur/sous-échantillonnage
- Évaluer avec AUC-PR plutôt que AUC-ROC

### Feature Importance
Le modèle affiche l'importance de chaque feature après l'entraînement pour comprendre quels facteurs influencent le plus les pourboires.

---

