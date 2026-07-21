CREATE TABLE dim_game
(
    game_key BIGINT PRIMARY KEY,
    year INT NOT NULL,
    Season VARCHAR(20) NOT NULL,
    City VARCHAR(100) NOT NULL,
    Date_insert TIMESTAMP NOT NULL
);