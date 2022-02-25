show dbs

use "country_vaccinations"

db.country_vaccinations_by_manufacturer.aggregate([ 
    {$project: {location:1, 
                date:{$convert:{input:"$date",to:"date"}},
                vaccine:1, 
                total_vaccinations:{$convert:{input:"$total_vaccinations", to:"double"}}
                }},
    {$out: "country_vaccinations_by_manufacturer"} 
    ])
 
db.country_vaccinations.aggregate([
    {$project:{
        country:1,
        iso_code:1,
        date:{$convert:{input:"$date",to:"date"}},
        total_vaccinations:{$convert:{input:"$total_vaccinations", to:"double"}},
        total_vaccinations_per_hundred:{$convert:{input:"$total_vaccinations_per_hundred", to:"double"}},
        daily_vaccinations:{$convert:{input:"$daily_vaccinations", to:"double"}},
        daily_vaccinations_per_million:{$convert:{input:"$daily_vaccinations_per_million", to:"double"}},
        people_fully_vaccinated_per_hundred:{$convert:{input:"$people_fully_vaccinated_per_hundred", to:"double"}},
        vaccines:1,
        source_name:1
        }
    },
    {$out: "country_vaccinations"}
    ])
    
db.covid19data.aggregate([
    {$project:{
        iso_code:1,
        continent:1,
        location:1,
        date:{$convert:{input:"$date",to:"date"}},
        total_cases:{$convert:{input:"$total_cases", to:"double"}},
        new_cases:{$convert:{input:"$new_cases", to:"double"}},
        daily_cases_per_million:{$convert:{input:"$daily_cases_per_million", to:"double"}},
        new_cases_per_million:{$convert:{input:"$new_cases_per_million", to:"double"}},
        stringency_index:{$convert:{input:"$stringency_index", to:"double"}},
        population:{$convert:{input:"$population", to:"double"}},
        population_density:{$convert:{input:"$population_density", to:"double"}},
        median_age:{$convert:{input:"$median_age", to:"double"}},
        age_65_older:{$convert:{input:"$age_65_older", to:"double"}},
        age_70_older:{$convert:{input:"$age_70_older", to:"double"}},
        gdp_per_capita:{$convert:{input:"$gdp_per_capita", to:"double"}},
        cardiovasc_death_rate:{$convert:{input:"$cardiovasc_death_rate", to:"double"}},
        diabetes_prevalence:{$convert:{input:"$diabetes_prevalence", to:"double"}},
        handwashing_facilities:{$convert:{input:"$handwashing_facilities", to:"double"}},
        hospital_beds_per_thousand:{$convert:{input:"$hospital_beds_per_thousand", to:"double"}},
        life_expentancy:{$convert:{input:"$life_expentancy", to:"double"}},
        human_development_index:{$convert:{input:"$human_development_index", to:"double"}},
        new_cases_smoothed:{$convert:{input:"$new_cases_smoothed", to:"double"}},
        new_deaths_smoothed:{$convert:{input:"$new_deaths_smoothed", to:"double"}},
        new_cases_smoothed_per_million:{$convert:{input:"$new_cases_smoothed_per_million", to:"double"}},
        new_deaths_smoothed_per_million:{$convert:{input:"$new_deaths_smoothed_per_million", to:"double"}},
        
    }
        
    },
    {$out: "covid19data"}
    
    ])

