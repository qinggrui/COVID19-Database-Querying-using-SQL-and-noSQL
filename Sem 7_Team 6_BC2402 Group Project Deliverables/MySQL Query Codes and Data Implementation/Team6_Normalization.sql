drop table if exists cases;
drop table if exists deaths;
drop table if exists hospital_patients;
drop table if exists hospital_admissions;
drop table if exists covid19_test;
drop table if exists vaccination;
drop table if exists country_info;
drop table if exists reproduction_rate;


create table `country` (
 `iso_code` varchar (15) ,
 `country` varchar(56),
 `continent` varchar(15),
  primary key (`iso_code`)	
  );
  
create table `cases` (
`iso_code` varchar(15) not null,
`date` date not null,
`total_cases` text,
`new_cases` text,
`new_cases_smoothed` text,
`total_cases_per_million` text,
`new_cases_per_million` text,
`new_cases_smoothed_per_million` text,
primary key (`iso_code`, `date`),
foreign key (`iso_code`) references `country` (`iso_code`)
);

create table `deaths` (
`iso_code` varchar(15) not null,
`date` date not null,
`total_deaths` text,
`new_deaths` text,
`new_deaths_smoothed` text,
`total_deaths_per_million` text,
`new_deaths_per_million` text,
`new_deaths_smoothed_per_million` text,
primary key (`iso_code`, `date`),
foreign key (`iso_code`) references `country` (`iso_code`)
);



create table `hospital_patients` (
`iso_code` varchar(15) not null,
`date` date not null,
`icu_patients` text,
`icu_patients_per_million` text,
`hosp_patients` text,
`hosp_patients_per_million` text,
primary key (`iso_code`, `date`),
foreign key (`iso_code`) references `country` (`iso_code`)
);

create table `hospital_admissions` (
`iso_code` varchar(15) not null,
`date` date not null,
`weekly_icu_admissions` text,
`weekly_icu_admissions_per_million` text,
`weekly_hosp_admissions` text,
`weekly_hosp_admissions_per_million` text,
primary key (`iso_code`, `date`),
foreign key (`iso_code`) references `country` (`iso_code`)
);

create table `covid19_test` (
`iso_code` varchar(15) not null,
`date` date not null,
`new_tests` text,
`total_tests` text,
`total_tests_per_thousand` text,
`new_tests_per_thousand` text,
`new_tests_smoothed` text,
`new_tests_smoothed_per_thousand` text,
`positive_rate` text,
`tests_per_case` text,
`tests_units` text,
primary key (`iso_code`, `date`),
foreign key (`iso_code`) references `country` (`iso_code`)
);

create table `vaccination` (
`iso_code` varchar(15) not null,
`date` date not null,
`total_vaccinations` text,
`people_vaccinated` text,
`people_fully_vaccinated` text,
`new_vaccinations` text,
`new_vaccinations_smoothed` text,
`total_vaccinations_per_hundred` text,
`people_vaccinated_per_hundred` text,
`people_fully_vaccinated_per_hundred` text,
`new_vaccinations_smoothed_per_million` text,
primary key (`iso_code`, `date`),
foreign key (`iso_code`) references `country` (`iso_code`)
);

create table `country_info` (
`iso_code` varchar(15) not null,
`date` date not null,
`stringency_index` text,
`population` text,
`population_density` text,
`median_age` text,
`aged_65_older` text,
`aged_70_older` text,
`gdp_per_capita` text,
`extreme_poverty` text,
`cardiovasc_death_rate` text,
`diabetes_prevalence` text,
`female_smokers` text,
`male_smokers` text,
`handwashing_facilities` text,
`hospital_beds_per_thousand` text,
`life_expectancy` text,
`human_development_index` text,
`excess_mortality` text,
primary key (`iso_code`, `date`),
foreign key (`iso_code`) references `country` (`iso_code`)
);

create table `reproduction_rate` (
`iso_code` varchar(15) not null,
`date` date not null,
`reproduction_rate` text,
primary key (`iso_code`, `date`),
foreign key (`iso_code`) references `country` (`iso_code`)
);


###INSERT
SET GLOBAL FOREIGN_KEY_CHECKS=0;
insert into `country`
select distinct iso_code, location, continent 
from covid19data;

insert into cases
 select iso_code, date, total_cases, new_cases, new_cases_smoothed,
 total_cases_per_million, new_cases_per_million, new_cases_smoothed_per_million
 from covid19data;

insert into `deaths`
select iso_code, date, total_deaths, new_deaths, 
new_deaths_smoothed, total_deaths_per_million, 
new_deaths_per_million, new_deaths_smoothed_per_million from covid19data;


insert into hospital_patients
select iso_code, `date`, icu_patients, icu_patients_per_million,  hosp_patients, hosp_patients_per_million
from covid19data;

insert into hospital_admissions
select iso_code, date, weekly_icu_admissions, weekly_icu_admissions_per_million, weekly_hosp_admissions, weekly_hosp_admissions_per_million
from covid19data;

insert into covid19_test
select iso_code, date, new_tests, total_tests, total_tests_per_thousand, new_tests_per_thousand, new_tests_smoothed, new_tests_smoothed_per_thousand, positive_rate, tests_per_case, tests_units
from covid19data;


insert into vaccination
select iso_code, date, total_vaccinations, people_vaccinated, people_fully_vaccinated, new_vaccinations, new_vaccinations_smoothed, total_vaccinations_per_hundred, people_vaccinated_per_hundred, people_fully_vaccinated_per_hundred, new_vaccinations_smoothed_per_million
from covid19data;

insert into country_info
select iso_code, `date`, stringency_index, population, population_density, median_age, aged_65_older, aged_70_older, gdp_per_capita, extreme_poverty, cardiovasc_death_rate, diabetes_prevalence, female_smokers, male_smokers, handwashing_facilities, hospital_beds_per_thousand, life_expectancy, human_development_index, excess_mortality
from covid19data;

insert into reproduction_rate
select iso_code,`date`, reproduction_rate
from covid19data;

### Drop unused columns in country_info
alter table `country_info`
drop column `stringency_index`,
drop column `median_age`,
drop column `aged_65_older`,
drop column `aged_70_older`,
drop column `gdp_per_capita`,
drop column `extreme_poverty`,
drop column `cardiovasc_death_rate`,
drop column `diabetes_prevalence`,
drop column `female_smokers`,
drop column `male_smokers`,
drop column `handwashing_facilities`,
drop column `hospital_beds_per_thousand`,
drop column `life_expectancy`,
drop column `human_development_index`,
drop column `excess_mortality`;
