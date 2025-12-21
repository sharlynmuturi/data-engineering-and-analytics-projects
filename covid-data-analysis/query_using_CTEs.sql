-- USING CTEs

-- Base Dataset for CovidDeaths
;WITH Deaths AS (
    SELECT continent, location, date, population, total_cases, new_cases, CAST(total_deaths AS FLOAT) AS total_deaths, CAST(new_deaths AS FLOAT) AS new_deaths
    FROM PortfolioProject..CovidDeaths
    WHERE continent IS NOT NULL
)

-- 1. Infection Analysis
-- Countries with Highest Infection Rate (Overall)
SELECT location, population, MAX(total_cases) AS highest_cases, MAX(total_cases * 1.0 / population) * 100 AS infection_rate
FROM Deaths
GROUP BY location, population
ORDER BY infection_rate DESC

-- Infection Rate Over Time - Kenya
SELECT date, population, total_cases, (total_cases * 1.0 / population) * 100 AS infection_rate
FROM Deaths
WHERE location = 'Kenya'
ORDER BY date

-- Peak Infection & Death Days
SELECT location, MAX(new_cases) AS peak_daily_cases, MAX(new_deaths) AS peak_daily_deaths
FROM Deaths
GROUP BY location

-- 2. Death Analysis
-- Global Death Rate Over Time
SELECT date, SUM(new_cases) AS total_cases, SUM(new_deaths) AS total_deaths, SUM(new_deaths) * 1.0 / NULLIF(SUM(new_cases), 0) * 100 AS death_rate
FROM Deaths
GROUP BY date
ORDER BY date

-- Fatality Rate - Continent-Level
SELECT continent, SUM(total_cases) AS total_cases, SUM(total_deaths) AS total_deaths
FROM Deaths
GROUP BY continent

--- Countries with Highest Total Deaths
SELECT location, MAX(total_deaths) AS total_deaths
FROM Deaths
GROUP BY location
ORDER BY total_deaths DESC

-- Case Fatality Rate - Kenya
SELECT date, total_cases, total_deaths, (total_deaths * 1.0 / NULLIF(total_cases, 0)) * 100 AS death_rate
FROM Deaths
WHERE location = 'Kenya'
ORDER BY date

-- 3. Vaccination Analysis
-- Rolling Vaccinations by Country
WITH VaccinationProgress AS (
    SELECT d.continent, d.location, d.date, d.population, v.new_vaccinations, SUM(CAST(v.new_vaccinations AS FLOAT))
            OVER (PARTITION BY d.location ORDER BY d.date) AS rolling_vaccinated
    FROM PortfolioProject..CovidDeaths d
    JOIN PortfolioProject..CovidVaccinations v ON d.location = v.location AND d.date = v.date
    WHERE d.continent IS NOT NULL
)

SELECT *, (rolling_vaccinated * 1.0 / population) * 100 AS vaccination_rate
FROM VaccinationProgress

-- 4. Vaccination vs Death Rate Correlation
SELECT d.location, MAX(v.people_fully_vaccinated_per_hundred) AS fully_vaccinated_pct, MAX(d.total_deaths * 1.0 / d.population) * 100 AS death_pct
FROM PortfolioProject..CovidDeaths d
JOIN PortfolioProject..CovidVaccinations v ON d.location = v.location AND d.date = v.date
WHERE d.continent IS NOT NULL
GROUP BY d.location

-- 5. Hospitalization Pressure
SELECT location, AVG(hosp_patients_per_million) AS avg_hospital_load
FROM PortfolioProject..CovidDeaths
WHERE continent IS NOT NULL
GROUP BY location
ORDER BY avg_hospital_load DESC



