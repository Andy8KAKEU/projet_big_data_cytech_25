-- =============================================================================
-- Suppression des tables existantes (Clean-up)
-- =============================================================================
DROP TABLE IF EXISTS FACT_TRIPS;

DROP TABLE IF EXISTS DIM_DATE;
DROP TABLE IF EXISTS DIM_LOCATION;


DROP TABLE IF EXISTS DIM_VENDOR;
DROP TABLE IF EXISTS DIM_RATE_CODE;
DROP TABLE IF EXISTS DIM_PAYMENT_TYPE;


-- 1. Tables de Dimensions de second niveau (Normalisation du flocon)

-- Dimension des fournisseurs (Vendor)
CREATE TABLE DIM_VENDOR (
    vendor_id INT PRIMARY KEY,
    vendor_name VARCHAR(50)
);

-- Dimension des types de tarifs (Rate Code)
CREATE TABLE DIM_RATE_CODE (
    rate_code_id INT PRIMARY KEY,
    rate_description VARCHAR(50)
);

-- Dimension des types de paiement
CREATE TABLE DIM_PAYMENT_TYPE (
    payment_type_id INT PRIMARY KEY,
    payment_description VARCHAR(50)
);

-- 2. Tables de Dimensions de premier niveau

-- Dimension temporelle (Date et Heure)
CREATE TABLE DIM_DATE (
    date_key TIMESTAMP PRIMARY KEY,
    hour INT,
    day INT,
    month INT,
    year INT,
    weekday VARCHAR(10)
);

-- Dimension géographique (Taxi Zones)
CREATE TABLE DIM_LOCATION (
    location_id INT PRIMARY KEY,
    borough VARCHAR(50),
    zone VARCHAR(100),
    service_zone VARCHAR(50)
);

-- 3. Table de Faits (Centrale)

CREATE TABLE FACT_TRIPS (
    trip_id SERIAL PRIMARY KEY,
    vendor_id INT REFERENCES DIM_VENDOR(vendor_id),
    pickup_datetime TIMESTAMP REFERENCES DIM_DATE(date_key),
    dropoff_datetime TIMESTAMP REFERENCES DIM_DATE(date_key),
    rate_code_id INT REFERENCES DIM_RATE_CODE(rate_code_id),
    pu_location_id INT REFERENCES DIM_LOCATION(location_id),
    do_location_id INT REFERENCES DIM_LOCATION(location_id),
    payment_type_id INT REFERENCES DIM_PAYMENT_TYPE(payment_type_id),
    store_and_fwd_flag VARCHAR(10),
    cbd_congestion_fee NUMERIC(10, 2),

    -- Mesures (Metrics)
    passenger_count INT,
    trip_distance FLOAT,
    fare_amount DECIMAL(10,2),
    extra DECIMAL(10,2),
    mta_tax DECIMAL(10,2),
    tip_amount DECIMAL(10,2),
    tolls_amount DECIMAL(10,2),
    improvement_surcharge DECIMAL(10,2),
    congestion_surcharge DECIMAL(10,2),
    airport_fee DECIMAL(10,2),
    total_amount DECIMAL(10,2)
);