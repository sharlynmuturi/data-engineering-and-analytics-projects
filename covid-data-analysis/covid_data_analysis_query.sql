-- COVID Deaths View
CREATE VIEW CovidDeathsView AS
SELECT continent, location, date, population, total_cases, new_cases, CAST(total_deaths AS FLOAT) AS total_deaths, CAST(new_deaths AS FLOAT) AS new_deaths
FROM PortfolioProject..CovidDeaths
WHERE continent IS NOT NULL

-- Global, Continent and Country Aggregates
SELECT continent, location, SUM(new_cases) AS total_cases, SUM(new_deaths) AS total_deaths, SUM(new_deaths)*1.0/SUM(new_cases)*100 AS death_rate
FROM CovidDeathsView
WHERE continent IS NOT NULL
GROUP BY continent, location
ORDER BY continent, location

-- Percentage of population infected
SELECT location, population, date, MAX(total_cases) AS highest_infection_count,  Max((total_cases/population))*100 AS percent_population_infected
FROM CovidDeathsView
GROUP BY location, population, date
ORDER BY percent_population_infected DESC

-- Top 10 countries by infection growth rate
SELECT TOP 10 location, MAX(total_cases * 1.0 / population) * 100 AS infection_rate
FROM CovidDeathsView
GROUP BY location
ORDER BY infection_rate DESC

-- Global Death Rate Over Time
SELECT date, SUM(new_cases) AS total_cases, SUM(new_deaths) AS total_deaths, SUM(new_deaths) * 1.0 / NULLIF(SUM(new_cases), 0) * 100 AS death_rate
FROM CovidDeathsView
GROUP BY date
ORDER BY date

-- Continent-level death rate over time
SELECT continent, date, SUM(new_cases) AS total_cases, SUM(new_deaths) AS total_deaths, SUM(new_deaths) * 1.0 / NULLIF(SUM(new_cases), 0) * 100 AS death_rate
FROM CovidDeathsView
GROUP BY continent, date
ORDER BY continent, date

--- Countries with Highest Total Deaths
SELECT location, MAX(total_deaths) AS total_deaths
FROM CovidDeathsView
GROUP BY location
ORDER BY total_deaths DESC

-- Countries with highest peak daily deaths
SELECT TOP 10 location, MAX(new_deaths) AS peak_daily_deaths
FROM CovidDeathsView
GROUP BY location
ORDER BY peak_daily_deaths DESC

-- Rolling death totals per country
SELECT location, date, SUM(new_deaths) OVER (PARTITION BY location ORDER BY date) AS rolling_deaths
FROM CovidDeathsView
ORDER BY location, date

-- Infection vs death rates per country
SELECT location, MAX(total_cases * 1.0 / population) * 100 AS infection_rate, MAX(total_deaths * 1.0 / population) * 100 AS death_rate
FROM CovidDeathsView
GROUP BY location
ORDER BY infection_rate DESC

-- Total cases vs total deaths (Case Fatality Rate per Continent)
SELECT continent, SUM(total_cases) AS total_cases, SUM(total_deaths) AS total_deaths, SUM(total_deaths) * 1.0 / NULLIF(SUM(total_cases),0) * 100 AS fatality_rate
FROM CovidDeathsView
GROUP BY continent
ORDER BY fatality_rate DESC

-- Case Fatality Rate per country per Country
SELECT location, MAX(total_deaths * 1.0 / NULLIF(total_cases, 0)) * 100 AS case_fatality_rate
FROM CovidDeathsView
GROUP BY location
ORDER BY case_fatality_rate DESC


-- First date country reached 1000 cases / 100 deaths
SELECT location, MIN(CASE WHEN total_cases >= 1000 THEN date END) AS date_1000_cases, MIN(CASE WHEN total_deaths >= 100 THEN date END) AS date_100_deaths
FROM CovidDeathsView
GROUP BY location
ORDER BY date_1000_cases

-- Countries with longest outbreak (days between first & last case)
SELECT location, DATEDIFF(DAY, MIN(date), MAX(date)) AS outbreak_duration_days
FROM CovidDeathsView
GROUP BY location
ORDER BY outbreak_duration_days DESC

-- COVID Vaccination amd Death View
CREATE OR ALTER VIEW VaccinationView AS
SELECT d.continent, d.location, d.date, d.population, CAST(v.new_vaccinations AS FLOAT) AS new_vaccinations,
    SUM(CAST(v.new_vaccinations AS FLOAT)) OVER (PARTITION BY d.location ORDER BY d.date) AS rolling_vaccinated,
    (SUM(CAST(v.new_vaccinations AS FLOAT)) OVER (PARTITION BY d.location ORDER BY d.date) * 1.0 / d.population) * 100 AS vaccination_rate
FROM PortfolioProject..CovidDeaths d
JOIN PortfolioProject..CovidVaccinations v ON d.location = v.location AND d.date = v.date
WHERE d.continent IS NOT NULL

SELECT *
FROM VaccinationView
ORDER BY location, date

-- Top vaccination rate per country
SELECT TOP 10 location, MAX(vaccination_rate) AS max_vaccination_rate
FROM VaccinationView
GROUP BY location
ORDER BY max_vaccination_rate DESC

-- Bottom 10 countries
SELECT TOP 10 location, MAX(vaccination_rate) AS max_vaccination_rate
FROM VaccinationView
GROUP BY location
ORDER BY max_vaccination_rate ASC

-- Average vaccination rate per continent over time
SELECT continent, date, AVG(vaccination_rate) AS avg_vaccination_rate
FROM VaccinationView
GROUP BY continent, date
ORDER BY continent, date

-- Vaccination vs to Death Rate per Country
SELECT v.location, v.date, v.vaccination_rate, d.total_deaths * 1.0 / d.population * 100 AS death_rate
FROM VaccinationView v
JOIN CovidDeathsView d ON v.location = d.location AND v.date = d.date
ORDER BY v.date

-- Global vaccination vs death rate over time
SELECT v.date, SUM(v.vaccination_rate * d.population) * 1.0 / SUM(d.population) AS global_vaccination_rate, SUM(d.total_deaths) * 1.0 / SUM(d.population) * 100 AS global_death_rate
FROM VaccinationView v
JOIN CovidDeathsView d ON v.location = d.location AND v.date = d.date
GROUP BY v.date
ORDER BY v.date

-- Max daily new vaccinations per country
SELECT location, MAX(new_vaccinations) AS peak_daily_vaccinations
FROM VaccinationView
GROUP BY location
ORDER BY peak_daily_vaccinations DESC

-- Growth Rate of Vaccinations per country
SELECT location, date, vaccination_rate,
    vaccination_rate - LAG(vaccination_rate) OVER (PARTITION BY location ORDER BY date) AS daily_growth_pct
FROM VaccinationView

ORDER BY location, date
