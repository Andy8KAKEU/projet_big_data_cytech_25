// ==========================
// Paramètres globaux
// ==========================
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.17"

// ==========================
// Dépendances communes
// ==========================
lazy val commonDependencies = Seq(
  "org.apache.spark" %% "spark-core" % "3.5.5",
  "org.apache.spark" %% "spark-sql" % "3.5.5",
  "org.apache.spark" %% "spark-mllib" % "3.5.5",
  "org.apache.hadoop" % "hadoop-aws" % "3.3.4",
  "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.262",
  "org.postgresql" % "postgresql" % "42.7.1"
)

// ==========================
// Projet common
// ==========================
lazy val common = project
  .in(file("common"))
  .settings(
    name := "common",
    libraryDependencies ++= commonDependencies
  )

// ==========================
// Projet ex1
// ==========================
lazy val ex1 = project
  .in(file("ex01_data_retrieval"))
  .dependsOn(common)
  .settings(
    name := "ex1",
    libraryDependencies ++= commonDependencies
  )

// ==========================
// Projet ex2
// ==========================
lazy val ex2 = project
  .in(file("ex02_data_ingestion"))
  .dependsOn(common)
  .settings(
    name := "ex2",
    libraryDependencies ++= commonDependencies
  )

// ==========================
// Projet ex4
// ==========================
lazy val ex4 = project
  .in(file("ex04_dashboard"))
  .dependsOn(common)
  .settings(
    name := "ex4",
    libraryDependencies ++= commonDependencies
  )

// ==========================
// Projet ex5
// ==========================
lazy val ex5 = project
  .in(file("ex05_ml_prediction_service"))
  .dependsOn(common)
  .settings(
    name := "ex5",
    libraryDependencies ++= commonDependencies
  )

// ==========================
// Projet racine (agrégateur)
// ==========================
lazy val root = project
  .in(file("."))
  .aggregate(ex1, ex2, ex4, ex5, common)
  .settings(
    name := "projet_ingestion",
    publish / skip := true
  )
