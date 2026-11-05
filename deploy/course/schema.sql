CREATE SCHEMA IF NOT EXISTS demo;
CREATE TABLE IF NOT EXISTS demo.notification_ground_truth (
    experiment_id TEXT NOT NULL,
    measurement_id TEXT NOT NULL,
    insert_timestamp TIMESTAMP DEFAULT now(),
    PRIMARY KEY (experiment_id, measurement_id)
);
CREATE TABLE IF NOT EXISTS demo.notification (
    experiment_id TEXT NOT NULL,
    measurement_id TEXT NOT NULL,
    group_id TEXT NOT NULL,
    latency DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (experiment_id, measurement_id, group_id)
);
