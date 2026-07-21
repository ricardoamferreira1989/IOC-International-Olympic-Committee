CREATE TABLE dim_event
(
    event_key BIGINT PRIMARY KEY,
    event_keyvent VARCHAR(200) NOT NULL,
    Sport VARCHAR(100) NOT NULL,
    date_insert TIMESTAMP NOT NULL
);