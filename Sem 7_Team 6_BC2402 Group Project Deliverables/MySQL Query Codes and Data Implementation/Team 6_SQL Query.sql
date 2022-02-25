#Q1
select continent, sum(p) as `Total_population_in_ Asia` from 
	(select country, continent, max(population) as p from country_info, country
	where country_info.iso_code = country.iso_code and
	continent = "Asia"
	group by country) as a;

#Q2
select continent, sum(p) as Total_population_in_ASEAN from 
	(select country, continent, max(population) as p from country_info, country
	where country_info.iso_code = country.iso_code and
	country in ("Singapore", "Malaysia", "Brunei", "Cambodia", "Indonesia",
				"Laos", "Myanmar", "Philippines", "Thailand", "Vietnam")
	group by country) as a;

#Q3
select distinct(source_name) as Data_sources, count(source_name) as Source_count
from country_vaccinations.country_vaccinations
group by source_name
order by source_name asc;
# Only country_vaccination table has information on data sources

#Q4
select `date`, daily_vaccinations as number_of_new_vaccinations_smoothed from country_vaccinations
where iso_code= (select iso_code
		from country 
		where country = "Singapore")
and
`date` between "3/01/2021" and "5/31/2021";
#Data obtained same as above
#Smoothed data is used rather than raw daily vaccinations
#Show trend using smoothed

#Q5
select country,min(str_to_date(date, "%m/%d/%Y")) as date,total_vaccinations
from country_vaccinations
where total_vaccinations > 0 and country = 'Singapore';


#Q6 (Output:3710)
select sum(new_cases) as total_number_of_cases
from cases
where iso_code in
(select iso_code
from country
where country = 'Singapore') and date >= '2021-01-11';


#Q7
select sum(new_cases) as total_number_of_cases
from cases
where iso_code in
(select iso_code
from country
where country = 'Singapore') and date <= '2021-01-10';


#Q8
drop view new_cases_percentage;

create view new_cases_percentage as
select cases.iso_code, cases.`date`, (new_cases_smoothed/population*100), population
from cases, country_info where
cases.iso_code = country_info.iso_code and
cases.`date` = country_info.`date` and
country_info.iso_code = "DEU";

select new_cases_percentage.`date`, new_cases_percentage.`(new_cases_smoothed/population*100)` as Percentage_of_new_cases, vaccine, (total_vaccinations/population*100) as Percentage_vaccinated
from new_cases_percentage, country_vaccinations_by_manufacturer where
country_vaccinations_by_manufacturer.`date` = new_cases_percentage.`date` and
country_vaccinations_by_manufacturer.location = "Germany"
group by new_cases_percentage.`date`,vaccine
order by new_cases_percentage.`date`, vaccine;


#Q9
select cases.`date` date1, new_cases_smoothed, C1.vaccine, C1.`date` date2, C1.total_vaccinations total_vaccinations1, C2.`date` date3, C2.total_vaccinations total_vaccinations2, C3.`date` date4, C3.total_vaccinations total_vaccinations3
from cases, country_vaccinations_by_manufacturer C1, country_vaccinations_by_manufacturer C2, country_vaccinations_by_manufacturer C3
where C2.`date` = date_add(C1.`date`, interval 10 DAY)
and C3.`date` = date_add(C1.`date`, interval 20 DAY)
and C1.`date` = date_add(cases.`date`, interval 20 DAY)
and C1.vaccine = C2.vaccine
and C1.vaccine = C3.vaccine
and C1.location = "Germany"
and C2.location = "Germany"
and C3.location = "Germany"
and cases.iso_code = (select iso_code 
			from country
			where country = "Germany")
; 

#Q10
select cm.`date` date1, vaccine, sum(total_vaccinations) sum_vaccinations, C1.`date` date2, C1.new_cases_smoothed, C2.`date` date3, C2.new_cases_smoothed, C3.`date` date4, C3.new_cases_smoothed
from country_vaccinations_by_manufacturer cm, cases C1, cases C2, cases C3
where C1.`date` = date_add(cm.`date`, interval 21 DAY)
and C2.`date` = date_add(cm.`date`, interval 60 DAY)
and C3.`date` = date_add(cm.`date`, interval 120 DAY)
and C1.iso_code = (select iso_code 
			from country
			where country = "Germany")
and C2.iso_code = (select iso_code 
			from country
			where country = "Germany")
and C3.iso_code = (select iso_code 
			from country
			where country = "Germany")
and location = "Germany"
group by date1, vaccine;
