-- The data was uploaded to BigQuery and whole script was run in BigQuery.

-- Looking at the dataset
SELECT *
FROM `sage-ship-364814.covid_data.covid_view`

-- Ordering the dataset
SELECT * FROM `sage-ship-364814.covid_data.covid_deaths` order by 3,4;

-- Getting specific columns from the dataset
SELECT location, date, total_cases, new_cases, total_deaths, population FROM `sage-ship-364814.covid_data.covid_deaths` ORDER BY 1,2;

-- Total cases vs Total deaths
-- Get likelihood of dying if you get infected by Covid in India
SELECT location, date, total_cases, total_deaths, (total_deaths/total_cases)*100 as death_percentage  
FROM `sage-ship-364814.covid_data.covid_deaths`
WHERE location = 'India'
and continent is not null
ORDER BY 2 DESC;


-- Total cases vs Population
-- Get percentage of population getting infected by Covid in India
SELECT location, date, total_cases, population, (total_cases/population)*100 as cases_percentage  
FROM `sage-ship-364814.covid_data.covid_deaths`
WHERE location = 'India'
and continent is not null
ORDER BY 5;


-- Get countries with highest percentage of population getting infected by Covid
SELECT location, population, MAX(total_cases) as highest_cases, MAX((total_cases/population))*100 as infected_percentage  
FROM `sage-ship-364814.covid_data.covid_deaths`
WHERE continent is not null
GROUP BY location, population
ORDER BY 4 DESC;  


-- Get countries with highest percentage of death in population
SELECT location, population, MAX(total_deaths) as highest_deaths, MAX((total_deaths/population))*100 as death_percentage  
FROM `sage-ship-364814.covid_data.covid_deaths`
WHERE continent is not null
GROUP BY location, population
ORDER BY 3 DESC;


-- Get continents with highest percentage of death in population
SELECT continent, MAX(total_deaths) as highest_deaths, MAX((total_deaths/population))*100 as death_percentage  
FROM `sage-ship-364814.covid_data.covid_deaths`
WHERE continent is not null
GROUP BY continent
ORDER BY 2 DESC;


-- Looking at Global Stats
--Death Percentages each day in 2021
SELECT date, SUM(new_cases) as total_cases, SUM(new_deaths) as total_deaths, SUM(new_deaths)/SUM(new_cases)*100 as death_percentage
FROM `sage-ship-364814.covid_data.covid_deaths`
WHERE continent is not null
GROUP BY date
ORDER BY 1 DESC;


--Death Percentages 0f 2021 aggregated at end
SELECT SUM(new_cases) as total_cases, SUM(new_deaths) as total_deaths, SUM(new_deaths)/SUM(new_cases)*100 as death_percentage
FROM `sage-ship-364814.covid_data.covid_deaths`
WHERE continent is not null
ORDER BY 1 DESC;


-- Vaccinations table
SELECT location, date, total_vaccinations, new_vaccinations FROM `sage-ship-364814.covid_data.covid_vaccinations` ORDER BY 1,2;


-- Merging tables using `Join`
--Total population vs total vaccinations
SELECT dea.continent, dea.location, dea.date, population, new_vaccinations
FROM `sage-ship-364814.covid_data.covid_deaths` dea
JOIN `sage-ship-364814.covid_data.covid_vaccinations` vac
  ON dea.location = vac.location
  and dea.date = vac.date
WHERE dea.continent is not null
ORDER BY 5 DESC;


-- New vaccinations in 2021 date wise
SELECT dea.continent, dea.location, dea.date, population, new_vaccinations
FROM `sage-ship-364814.covid_data.covid_deaths` dea
JOIN `sage-ship-364814.covid_data.covid_vaccinations` vac
  ON dea.location = vac.location
  and dea.date = vac.date
WHERE dea.continent is not null
and new_vaccinations is not null
ORDER BY 3;


-- Added rolling vaccinations (adding up as each day passes for each locations)
SELECT dea.continent, dea.location, dea.date, population, new_vaccinations, SUM(new_vaccinations) OVER (Partition by dea.location ORDER BY dea.location, dea.date) as rolling_vaccinations
FROM `sage-ship-364814.covid_data.covid_deaths` dea
JOIN `sage-ship-364814.covid_data.covid_vaccinations` vac
  ON dea.location = vac.location
  and dea.date = vac.date
WHERE dea.continent is not null
and new_vaccinations is not null
ORDER BY 2,3;



-- Using CTE
-- Using newly created column(rolling vaccinations) for another query using CTE
WITH temp_pop_vacc as 
(SELECT dea.continent, dea.location, dea.date, population, new_vaccinations, SUM(new_vaccinations) OVER (Partition by dea.location ORDER BY dea.location, dea.date) as rolling_vaccinations
FROM `sage-ship-364814.covid_data.covid_deaths` dea
JOIN `sage-ship-364814.covid_data.covid_vaccinations` vac
  ON dea.location = vac.location
  and dea.date = vac.date
WHERE dea.continent is not null
and new_vaccinations is not null)
SELECT *, (rolling_vaccinations/population)*100 as rolling_vaccination_percentage
FROM temp_pop_vacc;


-- Using Temp Table but cannot run as BILLING IS REQUIRED
-- Using newly created column(rolling vaccinations) for another query using Temp Table
DROP TABLE if exists `covid_data.Percent_vaccinated`;
CREATE TABLE `covid_data.Percent_vaccinated`
(
  `continent` string,
  `location` string,
  `date` datetime,
  `population` int64,
  `new_vaccinations` string,
  `rolling_vaccinations` float64
);
INSERT INTO `covid_data.Percent_vaccinated`
SELECT dea.continent, dea.location, dea.date, population, new_vaccinations, SUM(new_vaccinations) OVER (Partition by dea.location ORDER BY dea.location, dea.date) as rolling_vaccinations
FROM `sage-ship-364814.covid_data.covid_deaths` dea
JOIN `sage-ship-364814.covid_data.covid_vaccinations` vac
  ON dea.location = vac.location
  and dea.date = vac.date
WHERE dea.continent is not null
and new_vaccinations is not null;

SELECT *, (rolling_vaccinations/population)*100 as rolling_vaccination_percentage
FROM `covid_data.Percent_vaccinated`;



-- Created view using BigQuery without IAM permissions
--CREATE view `covid_view` as
SELECT dea.continent, dea.location, dea.date, population, new_vaccinations, SUM(new_vaccinations) OVER (Partition by dea.location ORDER BY dea.location, dea.date) as rolling_vaccinations
FROM `sage-ship-364814.covid_data.covid_deaths` dea
JOIN `sage-ship-364814.covid_data.covid_vaccinations` vac
  ON dea.location = vac.location
  and dea.date = vac.date
WHERE dea.continent is not null;