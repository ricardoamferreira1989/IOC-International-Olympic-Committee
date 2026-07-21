CREATE TABLE dim_athlete
(
    athlete_key BIGINT PRIMARY KEY,
    id INT NOT NULL,
    Name VARCHAR(200) NOT NULL,
    Sex CHAR(1),
    Age INT,
    Height INT,
    Weight INT,
    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP,
    current_flag BOOLEAN NOT NULL,
    date_insert TIMESTAMP NOT NULL
);
CREATE UNIQUE INDEX ux_dim_athlete_current
ON dim_athlete (athlete_id)
WHERE current_flag = TRUE;