DROP TABLE trip;
DROP TABLE station;
DROP TABLE weather;
DROP TABLE bike;

CREATE TABLE station (
	id INT PRIMARY KEY,
	name VARCHAR,
	latitude REAL,
	longtitude REAL
);

CREATE TABLE weather (
	id INT PRIMARY KEY,
	date DATE,
	avg_wind_speed REAL,
	precipitation REAL,
	snowfall REAL,
	snow_depth REAL,
	avg_temp REAL,
	max_temp REAL,
	min_temp REAL,
	fast_wind_2s_dir REAL,
	fast_wind_5s_dir REAL,
	fast_wind_2s_speed REAL,
	fast_wind_5s_speed REAL
);

CREATE TABLE bike (
	id INT PRIMARY KEY,
	number VARCHAR
);

CREATE TABLE trip (
	id SERIAL PRIMARY KEY,
	start_station_id INT REFERENCES station(id),
	end_station_id INT REFERENCES station(id),
	start_time TIMESTAMP,
	stop_time TIMESTAMP,
	bike_id INT REFERENCES bike(id),
	duration REAL,
	user_type VARCHAR,
	gender CHAR,
	birth_year INT
);