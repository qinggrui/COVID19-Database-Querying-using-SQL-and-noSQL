### Q1
SELECT DISTINCT `date`, total_vaccinations
FROM country_vaccinations
WHERE COUNTRY='Singapore'
GROUP BY`date`
ORDER BY `date` ASC
;

### Q2
SELECT country, SUM(daily_vaccinations) AS total_daily_vaccinations_ASEAN
FROM country_vaccinations
WHERE country
IN ('Singapore', 'Malaysia', 'Brunei', 'Cambodia', 'Indonesia', 'Laos', 'Myanmar', 'Philippines', 'Thailand', 'Vietnam')
GROUP BY country
ORDER BY country ASC
;
### Assume that question asking for the sum of all vaccinations in each of the 10 ASEAN Countries hence I found total vaccination count per country

### Q3
SELECT country, max(daily_vaccinations_per_million)
FROM country_vaccinations
GROUP BY country
ORDER BY max(daily_vaccinations_per_million) DESC
;

### Q4
SELECT vaccine, max(total_vaccinations) AS total_administrated
FROM country_vaccinations_by_manufacturer
GROUP BY vaccine
ORDER BY total_administrated DESC
;

### Q5 (2 Parts)
# Part 1:Identify first dates of each vaccine being administrated
SELECT vaccine, min(`date`) AS `date` 
FROM country_vaccinations_by_manufacturer
WHERE location='Italy'
GROUP BY vaccine
ORDER BY `date` ASC
;

# Part 2:Difference in days between earliest date and 4th date
SELECT 
	datediff(max(d.`date`), min(d.`date`)) AS 'Difference_In_Days'
FROM
	(SELECT 
		vaccine, 
		min(`date`) AS `date`
	FROM `country_vaccinations_by_manufacturer`
	WHERE location = "Italy"
	GROUP BY vaccine
	ORDER BY date ASC
	LIMIT 4) AS d
;

### Q6
SELECT m.location AS location, m.vaccine AS vaccine
FROM `country_vaccinations_by_manufacturer` AS m,
	(SELECT m.location, count(DISTINCT(m.vaccine)) AS 'vaccine_count'
	FROM `country_vaccinations_by_manufacturer` AS m
	GROUP BY m.location
    ORDER BY COUNT(DISTINCT(m.vaccine))) AS s
WHERE s.vaccine_count IN 
	(SELECT MAX(s.vaccine_count) 
	FROM (SELECT m.location,
		COUNT(DISTINCT(m.vaccine)) as 'vaccine_count'
	FROM `country_vaccinations_by_manufacturer` as m
	GROUP BY m.location
    ORDER BY COUNT(DISTINCT(m.vaccine))) as s)
AND s.location = m.location
GROUP BY m.location, m.vaccine
ORDER BY count(m.vaccine) DESC
;

### Q7
SELECT country, vaccines, max(people_fully_vaccinated_per_hundred) AS vaccinated_percentage
FROM country_vaccinations
WHERE people_fully_vaccinated_per_hundred>60
GROUP BY country
ORDER BY vaccinated_percentage DESC
;
### Assume that Gibraltar percentage >100 is normal because they vaccinate other countries and tourists too


### Q8
SELECT LEFT(monthname(`date`),3) as month, vaccine, max(total_vaccinations) as monthly_total_vaccinations
FROM country_vaccinations_by_manufacturer
WHERE location ='United States'
GROUP BY vaccine, Month(`date`)
ORDER BY month(`date`) ASC
;

### Q9
SELECT 
	t.country as 'country',
    DATEDIFF( s.first_vaccination_date_over_50, t.first_vaccination_date) as 'Days_to_over_50%'
FROM (SELECT cv.country as 'country',
    min(str_to_date(cv.date, "%m/%d/%Y")) as 'first_vaccination_date'
	FROM country_vaccinations as cv
	GROUP BY cv.country) as t
INNER JOIN
(SELECT 
	cv.country as 'country',
    min(str_to_date(cv.date, "%m/%d/%Y")) as 'first_vaccination_date_over_50'
	FROM country_vaccinations as cv
	WHERE cv.total_vaccinations_per_hundred >50
GROUP BY cv.country) as s
ON t.country = s.country
;


### Q10
SELECT g.vaccine, sum(total_vaccinations) AS global_total
FROM country_vaccinations_by_manufacturer AS g
INNER JOIN (SELECT location, vaccine, max(`date`) as vaxdate
			FROM country_vaccinations_by_manufacturer as g
			GROUP BY g.location, g.vaccine) as m 
			ON g.vaccine = m.vaccine 
			AND g.location = m.location
			AND g.date=m.vaxdate
GROUP BY g.vaccine
ORDER BY global_total DESC
;



