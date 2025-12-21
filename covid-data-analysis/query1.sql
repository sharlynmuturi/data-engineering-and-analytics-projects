SELECT *
FROM PortfolioProject..CovidDeaths

-- Infection Rate 
-- Infection Rate in Kenya

SELECT Location, date, population, total_cases, (total_cases/population)*100 AS InfectionRate
FROM PortfolioProject..CovidDeaths
WHERE location LIKE '%kenya%'
ORDER BY InfectionRate DESC

-- Countries with Highest Infection Rate

SELECT Location, population, MAX(total_cases) AS HighestInfectionCount, MAX((total_cases/population))*100 AS InfectionRate
FROM PortfolioProject..CovidDeaths
WHERE continent IS NOT NULL
GROUP BY Location, population
ORDER BY InfectionRate DESC

-- Death Rate
SELECT Location, date, total_cases, new_cases, total_deaths, population
FROM PortfolioProject..CovidDeaths
WHERE continent IS NOT NULL
ORDER BY 1, 2

-- Death Rate in Kenya

SELECT Location, date, total_cases, total_deaths, (total_deaths/total_cases)*100 AS DeathRate
FROM PortfolioProject..CovidDeaths
WHERE location LIKE '%kenya%'
ORDER BY 1, 2

-- Countries with highest number of death

SELECT Location, MAX(CAST(total_deaths AS INT)) AS TotalDeathsCount
FROM PortfolioProject..CovidDeaths
WHERE continent IS NOT NULL
GROUP BY Location
ORDER BY TotalDeathsCount DESC

SELECT Location, MAX(CAST(total_deaths AS INT)) AS TotalDeathsCount
FROM PortfolioProject..CovidDeaths
WHERE continent IS NULL
GROUP BY Location
ORDER BY TotalDeathsCount DESC

-- Continents with highest number of death

SELECT Continent, MAX(CAST(total_deaths AS INT)) AS TotalDeathsCount
FROM PortfolioProject..CovidDeaths
WHERE continent IS NOT NULL
GROUP BY Continent
ORDER BY TotalDeathsCount DESC


-- Death Rate Globally
SELECT SUM(new_cases) AS TotalCases, SUM(CAST(new_deaths AS INT)) AS TotalDeaths, SUM(CAST(new_deaths AS INT))/SUM(new_cases)*100 AS DeathRate
FROM PortfolioProject..CovidDeaths
WHERE continent IS NOT NULL
ORDER BY 1, 2

SELECT date, SUM(new_cases) AS TotalCases, SUM(CAST(new_deaths AS INT)) AS TotalDeaths
FROM PortfolioProject..CovidDeaths
WHERE continent IS NOT NULL
GROUP BY date
ORDER BY 1, 2

SELECT date, SUM(new_cases) AS TotalCases, SUM(CAST(new_deaths AS INT)) AS TotalDeaths, SUM(CAST(new_deaths AS INT))/SUM(new_cases)*100 AS DeathRate
FROM PortfolioProject..CovidDeaths
WHERE continent IS NOT NULL
GROUP BY date
ORDER BY 1, 2

-- Vaccination Rate

SELECT *
FROM PortfolioProject..CovidDeaths deaths
JOIN PortfolioProject..CovidVaccinations vaccinations
ON deaths.Location = vaccinations.Location AND deaths.date = vaccinations.date

SELECT deaths.continent, deaths.Location, deaths.date, deaths.population, vaccinations.new_vaccinations, SUM(CONVERT(int,vaccinations.new_vaccinations))
OVER (PARTITION BY deaths.location ORDER BY deaths.location, deaths.date) AS RollingPeopleVaccinated
FROM PortfolioProject..CovidDeaths deaths
JOIN PortfolioProject..CovidVaccinations vaccinations
ON deaths.Location = vaccinations.Location AND deaths.date = vaccinations.date
WHERE deaths.continent IS NOT NULL
ORDER BY 2, 3

-- USE CTE

WITH PopvsVac (Continent, Location, Date, Population, new_vaccinations, RollingPeopleVaccinated)
AS(
	SELECT deaths.continent, deaths.Location, deaths.date, deaths.population, vaccinations.new_vaccinations, SUM(CONVERT(int,vaccinations.new_vaccinations))
	OVER (PARTITION BY deaths.location ORDER BY deaths.location, deaths.date) AS RollingPeopleVaccinated
	FROM PortfolioProject..CovidDeaths deaths
	JOIN PortfolioProject..CovidVaccinations vaccinations
	ON deaths.Location = vaccinations.Location AND deaths.date = vaccinations.date
	WHERE deaths.continent IS NOT NULL
)

SELECT *, (RollingPeopleVaccinated/Population) * 100
FROM PopvsVac

DROP TABLE IF EXISTS #VaccinationRate
--TEMP TABLE
CREATE TABLE #VaccinationRate
(
continent NVARCHAR(255),
location NVARCHAR(255),
date datetime,
population numeric,
new_vaccinations numeric,
RollingPeopleVaccinated numeric
)

INSERT INTO #VaccinationRate
SELECT deaths.continent, deaths.Location, deaths.date, deaths.population, vaccinations.new_vaccinations, SUM(CONVERT(int,vaccinations.new_vaccinations))
OVER (PARTITION BY deaths.location ORDER BY deaths.location, deaths.date) AS RollingPeopleVaccinated
FROM PortfolioProject..CovidDeaths deaths
JOIN PortfolioProject..CovidVaccinations vaccinations
ON deaths.Location = vaccinations.Location AND deaths.date = vaccinations.date
WHERE deaths.continent IS NOT NULL

SELECT *, (RollingPeopleVaccinated/Population) * 100
FROM #VaccinationRate

-- Creating view to store data or later visualizations

CREATE VIEW #VaccinationRate AS
SELECT deaths.continent, deaths.Location, deaths.date, deaths.population, vaccinations.new_vaccinations, SUM(CONVERT(int,vaccinations.new_vaccinations))
OVER (PARTITION BY deaths.location ORDER BY deaths.location, deaths.date) AS RollingPeopleVaccinated
FROM PortfolioProject..CovidDeaths deaths
JOIN PortfolioProject..CovidVaccinations vaccinations
ON deaths.Location = vaccinations.Location AND deaths.date = vaccinations.date
WHERE deaths.continent IS NOT NULL
