show dbs

use "country_vaccinations"

//q1: Display a list of total vaccinations per day in Singapore//

db.country_vaccinations.aggregate([
    {$match:{country:"Singapore"}},
    {$sort:{"date":1}},
    {$project:{_id:0, country:1, total_vaccinations:1, date:1}}
    ])


//q2:Display the sum of daily vaccinations among ASEAN countries.//

db.country_vaccinations.aggregate([
    {$project: {_id:0, country:1, daily_vaccinations:1}},
    {$match:{country:{$in: ["Singapore", "Malaysia", "Brunei", "Cambodia", "Indonesia",
"Laos", "Myanmar", "Philippines", "Thailand", "Vietnam"]}}},
    {$group: { _id:{country: "$country"}, total_administered:{$sum:"$daily_vaccinations"}}},
    {$sort: {total_administered:-1}}
    ])


//q3:Identify the maximum daily vaccinations per million of each country. Sort the list based on daily vaccinations per million in a descending order//

db.country_vaccinations.aggregate([
    {$project: {_id:0, country:1, daily_vaccinations_per_million:1}},
    {$group: { _id:{country: "$country"}, Max_daily_vaccinations_per_million:{$max: "$daily_vaccinations_per_million"}}},
    {$sort: {Max_daily_vaccinations_per_million:-1}}
    ])


//q4: Which is the most administered vaccine? Display a list of total administration (i.e., sum of total vaccinations) per vaccine.//

db.getCollection("country_vaccinations_by_manufacturer").aggregate([
    {$project: {_id:0, vaccine:1,location:1, total_vaccinations:1}},
    {$group: { _id:{location: "$location", vaccine:"$vaccine"}, 
            Max_total_vaccination:{$max:"$total_vaccinations"}}},
    {$group: { _id: {vaccine:"$_id.vaccine"},
            total_administered:{$sum:"$Max_total_vaccination"}}},
    {$sort: {total_administered: -1}}
    ])


//q5: Italy has commenced administering various vaccines to its populations as a vaccine becomes available.
// Identify the first dates of each vaccine being administered, then compute the difference in days between the earliest date and the 4th date.//

db.country_vaccinations_by_manufacturer.aggregate([ 
    // inner query //
    {$match:{"location":"Italy"}},
    {$group:{_id:{vaccine:"$vaccine"},
        date:{$min:"$date"}}},
    {$project:{_id:0, vaccine:"$_id.vaccine", date:"$date"}},
    
    // outer query //
    {$group:{ _id:{},
        "minDate": { "$min": "$date" },"maxDate": { "$max": "$date" }}},
    {$project:{_id:0, Difference_In_Days:{$divide:[{$subtract:["$maxDate","$minDate"]},1000*60*60*24]}}}
    ])


//q6: What is the country with the most types of administered vaccine?//

db.getCollection("country_vaccinations_by_manufacturer").aggregate([ 
    {$project:{_id:0, location:1, vaccine:1}}, 
    {$group:{_id:{location:"$location"}, vaccine_types: {$addToSet: "$vaccine"}}},  
    {$project: {_id:1, vaccine:1, vaccine_types: {$size: "$vaccine_types"}}}, 
    {$sort: {vaccine_types: -1}}, 
    {$limit: 1} 
    ])
    
//q7: What are the countries that have fully vaccinated more than 60% of its people? For each country, display the vaccines administered.//

db.getCollection("country_vaccinations").aggregate([
    {$project: {_id:0, country:1, vaccines:1, 
    people_fully_vaccinated_per_hundred:1}},
    {$match:{people_fully_vaccinated_per_hundred:{$gt:60}}},
    {$group: {_id:{country: "$country",vaccines:"$vaccines"}, vaccinated_percentage:{$max:"$people_fully_vaccinated_per_hundred"}}},
    {$sort: {vaccinated_percentage:-1}}
    ])


//q8: Monthly vaccination insight – display the monthly total vaccination amount of each vaccine per month in the United States.//

db.getCollection("country_vaccinations_by_manufacturer").aggregate([
    {$match: {location: "United States"}},
    {$project: {_id:0, date: {$month: "$date"}, vaccine:1, total_vaccinations:1}},
    {$group: {_id:{ month:"$date", vaccine: "$vaccine"}, monthly_total_vaccination: {$max: "$total_vaccinations"}}},
    {$sort: {_id:1}}
    ])


//q9: Days to 50 percent. Compute the number of days (i.e., using the first available date on records of a country) //
//that each country takes to go above the 50% threshold of vaccination administration (i.e., total_vaccinations_per_hundred > 50)//

db.getCollection("country_vaccinations").aggregate([ 
    {$group:{ 
        _id:{Country:"$country"}, 
        "First_date":{$min:"$date"}, 
        "DailyPercentages":{$push:{"date":"$date", 
        "Vaccinations_per_hundred":"$total_vaccinations_per_hundred"}}}}, 
    {$project:{ 
        _id:0, 
        First_date:1, 
        Country:"$_id.Country", 
        DailyPercentages2:{$filter:{ 
            input:"$DailyPercentages", 
            as: "a", 
            cond: {$gt:["$$a.Vaccinations_per_hundred",50]} 
        }}}}, 
    {$project:{_id:0,Country:1,DaysOverFifty:{$dateDiff:{startDate:"$First_date",endDate:{$min:"$DailyPercentages2.date"},unit:"day"}}}}, 
    {$match:{"DaysOverFifty":{$ne:null}}}, 
    {$sort:{Country:1}} 
])


//q10: Compute the global total of vaccinations per vaccine.//

db.getCollection("country_vaccinations_by_manufacturer").aggregate([
    {$project: {_id:0, vaccine:1,location:1, total_vaccinations:1}},
    {$group: { _id:{location: "$location", vaccine:"$vaccine"}, 
            Max_total_vaccination:{$max:"$total_vaccinations"}}},
    {$group: { _id: {vaccine:"$_id.vaccine"},
            total_administered:{$sum:"$Max_total_vaccination"}}},
    {$sort: {total_administered: -1}}
    ])


//q11: What is the total population in Asia?//

db.getCollection("covid19data").aggregate([
    {$match: {continent: "Asia"}},
    {$project: {location:1, continent:1, population:1}},
    {$group: {_id: {location: "$location", continent:"$continent"}, max_population:{$max: "$population"}}}, 
    {$group: { _id: "$_id.continent", total_population:{$sum: "$max_population"}}} 
    ])


//q12: What is the total population among the ten ASEAN countries?//

db.getCollection("covid19data").aggregate([
    {$match: {location:{$in: ["Singapore", "Malaysia", "Brunei", "Cambodia", "Indonesia",
"Laos", "Myanmar", "Philippines", "Thailand", "Vietnam"]}}},
    {$project: {location:1, continent:1, population:1}},
    {$group: {_id: {location: "$location", continent:"$continent"}, country_population:{$max: "$population"}}},
    {$group: {_id: "$_id.continent", total_population_in_ASEAN:{$sum: "$country_population"}}} 
    ])


//q13: Generate a list of unique data sources (source_name)//
 db.country_vaccinations.aggregate([
    {$group:{_id:{source_name:"$source_name"},totalSourceCount:{$sum:1}}},
    {$sort:{totalSourceCount:-1}},
    {$project:{ source_name:1, totalSourceCount:1}} 
    ])


//q14:Specific to Singapore, display the daily total_vaccinations starting (inclusive) March-1 2021 through (inclusive) May-31 2021.//

db.getCollection("country_vaccinations").aggregate([
    {$match: {country:"Singapore"}},
    {$match: {"date": {$gte: ISODate('2021-03-01T00:00:00.000+08:00'), $lte: ISODate('2021-06-01T00:00:00.000+08:00')}}},
    {$project: {_id:0, country:1, date: 1, daily_vaccinations:1}}
    ])


//q15: When is the first batch of vaccinations recorded in Singapore?//

db.getCollection("country_vaccinations").aggregate([
    {$match: {country:"Singapore"}},
    {$group: { _id: "$country", First_vaccination: {$min: "$date"}}},
    {$project: {_id:0, First_vaccination:1}}
    ])


//q16: Based on the date identified in (5), specific to Singapore, compute the total number of new cases thereafter.
// For instance, if the date identified in (5) is Jan-1 2021, the total number of new cases will be the sum of new cases starting from (inclusive) Jan-1 to the last date in the dataset.

db.getCollection("covid19data").aggregate([
    {$project: {location:1, date:1, new_cases:1}},
    {$match: {location:"Singapore"}},
    {$match: {"date": {$gte: ISODate('2021-01-11T00:00:00.000+08:00')}}},
    {$group: { _id:{}, total_new_cases: {$sum: "$new_cases"}}},
    {$project: {_id:0, total_new_cases:1}}
    ])


//q17: For instance, if the date identified in (5) is Jan-1 2021 and the first date recorded (in Singapore) in the dataset is Feb-1 2020,
// the total number of new cases will be the sum of new cases starting from (inclusive) Feb-1 2020 through (inclusive) Dec-31 2020.

db.getCollection("covid19data").aggregate([
    {$project: {location:1, date: 1, new_cases: 1}},
    {$match: {location:"Singapore"}},
    {$match: {"date": {$lt: ISODate('2021-01-11T00:00:00.000+08:00')}}},
    {$group: { _id:{}, total_new_cases: {$sum: "$new_cases"}}},
    {$project: {_id:0, total_new_cases:1}}
    ])


//q18: Herd immunity estimation. On a daily basis, specific to Germany, calculate the percentage of new cases and total vaccinations on each available vaccine in relation to its population.//

db.getCollection("covid19data").aggregate([
    {$match: {location: "Germany"}},
    {$lookup: {
        from: "country_vaccinations_by_manufacturer",
        localField: "date",
        foreignField: "date",
        pipeline: [
            {$match: {location:"Germany"}},
            {$project: {location:1 ,date:1, vaccine:1, total_vaccinations:1}}
            ],
            as: "a"
    }},
    {$unwind: "$a"},
    {$project: {_id:0, location:1, date:1, vaccine: "$a.vaccine", total_vaccinations: "$a.total_vaccinations", new_cases_smoothed:1 , population:1}},
    {$addFields: {percentage_of_new_cases: {$multiply: [ {$divide: ["$new_cases_smoothed","$population"]},100]},
        percentage_of_total_vaccinations: {$multiply: [ {$divide: ["$total_vaccinations","$population"]},100]}}},
    {$group: { _id: {location:"$location", date:"$date", vaccine:"$vaccine", percentage_of_new_cases:"$percentage_of_new_cases", percentage_of_total_vaccinations:"$percentage_of_total_vaccinations"}}},
    {$sort:{_id: 1}},
])


//q19: Vaccination Drivers. Specific to Germany, based on each daily new case, display the total vaccinations of each available vaccines after 20 days, 30 days, and 40 days.//

db.getCollection("covid19data").aggregate([ 
    {$match:{location:"Germany"}}, 
    {$project:{ 
        _id:0, 
        location:1, 
        date:1, 
        new_cases_smoothed:1, 
        "20Days":{$add:["$date",1000*60*60*24*20]}, 
        "30Days":{$add:["$date",1000*60*60*24*30]}, 
        "40Days":{$add:["$date",1000*60*60*24*40]} 
    }}, 
    {$lookup:{ 
        from:"country_vaccinations_by_manufacturer", 
        localField:"20Days", 
        foreignField:"date", 
        pipeline:[ 
            {$match:{location:"Germany"}}, 
            {$project:{_id:0,date:1,vaccine:1,total_vaccinations:1}} 
            ], 
            as: "20DaysVax" 
    }}, 
    {$lookup:{ 
        from:"country_vaccinations_by_manufacturer", 
        localField:"30Days", 
        foreignField:"date", 
        pipeline:[ 
            {$match:{location:"Germany"}}, 
            {$project:{_id:0,date:1,vaccine:1,total_vaccinations:1}} 
            ], 
            as: "30DaysVax" 
    }}, 
    {$lookup:{ 
        from:"country_vaccinations_by_manufacturer", 
        localField:"40Days", 
        foreignField:"date", 
        pipeline:[ 
            {$match:{location:"Germany"}}, 
            {$project:{_id:0,date:1,vaccine:1,total_vaccinations:1}} 
            ], 
            as: "40DaysVax" 
    }}, 
    {$match:{"20DaysVax":{$ne:[]}}}, /* before here if no need to display nicely */ 
    {$project:{_id:0,country:1,date:1,new_cases_smoothed:1,"20DaysVax":"$20DaysVax","30DaysVax":"$30DaysVax","40DaysVax":"$40DaysVax"}}, 
    {$match:{"30DaysVax":{$ne:[]}}}, 
    {$match:{"40DaysVax":{$ne:[]}}} 
])
 
 
//q20: Vaccination Effects. Specific to Germany, on a daily basis, based on the total number of accumulated vaccinations (sum of total_vaccinations of each vaccine in a day),
// generate the daily new cases after 21 days, 60 days, and 120 days.//

db.getCollection("covid19data").aggregate([ 
    {$match:{location:"Germany"}},
    {$project:{ 
        _id:0, 
        location:1, 
        date:1,
        "21Days":{$add:["$date",1000*60*60*24*21]}, 
        "60Days":{$add:["$date",1000*60*60*24*60]}, 
        "120Days":{$add:["$date",1000*60*60*24*120]} 
    }}, 
    {$lookup:{ 
        from:"country_vaccinations_by_manufacturer", 
        localField:"date", 
        foreignField:"date", 
        pipeline:[ 
            {$match:{location:"Germany"}},
            {$project:{_id:0,date:1,vaccine:1, total_vaccinations:1}} 
            ], 
            as: "Vaccine_Count" 
    }},
    {$lookup:{ 
        from:"covid19data", 
        localField:"21Days", 
        foreignField:"date", 
        pipeline:[ 
            {$match:{location:"Germany"}},
            {$project:{_id:0, new_cases_smoothed:1, date:1}}
            ], 
            as: "21DaysCases" 
    }},     
    {$lookup:{ 
        from:"covid19data", 
        localField:"60Days", 
        foreignField:"date", 
        pipeline:[ 
            {$match:{location:"Germany"}}, 
            {$project:{_id:0,new_cases_smoothed:1, date:1}} 
            ], 
            as: "60DaysCases" 
    }},
        {$lookup:{ 
        from:"covid19data", 
        localField:"120Days", 
        foreignField:"date", 
        pipeline:[ 
            {$match:{location:"Germany"}}, 
            {$project:{_id:0,new_cases_smoothed:1, date:1}} 
            ], 
            as: "120DaysCases" 
    }},     
    {$match:{"21DaysCases":{$ne:[]}}}, /* before here if no need to display nicely */ 
    {$match:{"60DaysCases":{$ne:[]}}}, 
    {$match:{"120DaysCases":{$ne:[]}}},
    {$match:{"Vaccine_Count":{$ne:[]}}},
    {$match:{location:"Germany"}},
    {$project:{_id:0,country:1,date:1,
    "Vaccine_Count":"$Vaccine_Count",
    "21DaysCases":"$21DaysCases",
    "60DaysCases":"$60DaysCases",
    "120DaysCases":"$120DaysCases"}}
])
