CREATE VIEW trips_per_month AS
SELECT
	to_char(w.date, 'Month') AS month,
	COUNT(*) AS trip_count
FROM trip t
JOIN weather w ON date(t.start_time) = date(w.date)
GROUP BY 1
ORDER BY 2 DESC;

CREATE VIEW trips_per_season AS
SELECT
	CASE
		WHEN to_char(w.date, 'MM') IN ('12', '01', '02') THEN 'Winter'
		WHEN to_char(w.date, 'MM') IN ('03', '04', '05') THEN 'Spring'
		WHEN to_char(w.date, 'MM') IN ('06', '07', '08') THEN 'Summer'
		WHEN to_char(w.date, 'MM') IN ('09', '10', '11') THEN 'Fall'
		ELSE 'ERROR'
	END AS season,
	COUNT(*) AS trip_count
FROM trip t
JOIN weather w ON date(t.sZtart_time) = date(w.date)
GROUP BY 1
ORDER BY 2 DESC;

CREATE VIEW avg_duration_per_month AS
SELECT
	to_char(w.date, 'Month') AS month,
	AVG(t.duration) AS avg_duration
FROM trip t
JOIN weather w ON date(t.start_time) = date(w.date)
GROUP BY 1
ORDER BY 2 DESC;

CREATE VIEW trips_per_hour AS
SELECT
	EXTRACT(hour FROM start_time) AS hour,
	COUNT(*) AS trip_count
FROM trip t
GROUP BY 1
ORDER BY 2 DESC;

CREATE VIEW trips_per_temp_range AS
WITH temp_for_six AS (
	SELECT
		NTILE(6) OVER (ORDER by avg_temp) AS heptile,
		avg_temp,
		date(date)
	FROM weather
)
SELECT
	t6.heptile,
	MIN(avg_temp) AS lowest_temp,
	MAX(avg_temp) AS highest_temp,
	COUNT(*) AS trip_count
FROM trip t
JOIN temp_for_six t6 ON date(t.start_time) = t6.date
GROUP BY 1 ORDER BY 1;

CREATE VIEW trips_per_wind_speed AS
WITH wind_speed_for_4 AS (
	SELECT
		NTILE(4) OVER (ORDER by avg_wind_speed) AS part,
		avg_wind_speed,
		date(date)
	FROM weather
)
SELECT
	t4.part,
	MIN(avg_wind_speed) AS lowest_wind_speed,
	MAX(avg_wind_speed) AS highest_wind_speed,
	COUNT(*) AS trip_count
FROM trip t
JOIN wind_speed_for_4 t4 ON date(t.start_time) = t4.date
GROUP BY 1 ORDER BY 1;

CREATE VIEW trips_with_prec AS
WITH days_prec AS (
	SELECT
		'Precipitation' AS prec,
		COUNT(*) AS prec_count
	FROM weather
	WHERE precipitation > 0
	UNION
	SELECT
		'No precipitation' AS prec,
		COUNT(*) AS prec_count
	FROM weather
	WHERE precipitation = 0
), trip_prec AS (
	SELECT
		'Precipitation' AS prec,
		COUNT(*) AS trip_count,
		COUNT(DISTINCT date(t.start_time)) AS day_count
	FROM trip t
	JOIN weather w ON date(t.start_time) = date(w.date)
	WHERE precipitation > 0
	UNION
	SELECT
		'No precipitation' AS prec,
		COUNT(*) AS trip_count,
		COUNT(DISTINCT date(t.start_time)) AS day_count
	FROM trip t
	JOIN weather w ON date(t.start_time) = date(w.date)
	WHERE precipitation = 0
)
SELECT
	t.prec, t.trip_count, t.day_count,
	1.0 * t.trip_count / t.day_count AS trips_per_day,
	d.prec_count AS total_days_prec,
	100.0 * (d.prec_count - t.day_count) / d.prec_count AS unused_days_percentage
FROM trip_prec t
JOIN days_prec d on t.prec = d.prec;

