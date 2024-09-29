-- Data cleaning --

-- SELECT *
-- FROM layoffs;

-- 1. Remove duplicates --
-- 2. Standardize the data --
-- 3. Null Values or blank value --
-- 4. Remove any columns --

 -- create backup DB of the raw data to avoid any alteration of the raw data in the future, mandatory step --
-- CREATE TABLE layoffs_staging
-- LIKE layoffs;

-- INSERT layoffs_staging
-- SELECT *
-- FROM layoffs;
-----------------------------------------------------------
-- 1. Remove duplicates --

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1
;

 -- to check if the entries are actually duplicate --
 
--  SELECT *
--  FROM layoffs_staging
--  WHERE company = 'Casper'
--  ;

-- deleting the entries where row number is 2. for that create another table and insert the rows of the staging table --

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2
WHERE row_num > 2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

DELETE
FROM layoffs_staging2
where row_num > 1;

 -- 2. Standardizing Data - remove spaces, check every column with the help of distinct keyword --
 
 Select company, TRIM(company)
-- 					location, TRIM(location),
-- 					industry, TRIM(industry),
-- 					total_laid_off, TRIM(total_laid_off),
-- 					percentage_laid_off, TRIM(percentage_laid_off)
 FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

 Select *
 FROM layoffs_staging2
 WHERE industry LIKE 'Crypto%';
 
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

 Select DISTINCT country
 FROM layoffs_staging2
 ORDER BY 1;
 
Select DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- Since date is in text we have to convert it to date type --

Select 	`date`
-- str_to_date(`date`,'%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = str_to_date(`date`,'%m/%d/%Y')
;

ALTER TABLE layoffs_staging2
modify COLUMN `date` DATE;

-- 3. Removing NULLS--
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

SELECT *
FROM layoffs_staging2 a1
JOIN layoffs_staging2 b1
ON a1.company = b1.company
AND a1.location = b1.location
WHERE (a1.industry IS NULL OR a1.industry = '')
AND b1.industry IS NOT NULL;

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

UPDATE layoffs_staging2 a1
JOIN layoffs_staging2 b1
ON a1.company = b1.company
SET a1.industry = b1.industry
WHERE (a1.industry IS NULL)
AND b1.industry IS NOT NULL;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2;

-- 4. Dropping unnecessary columns --
ALTER table layoffs_staging2
DROP COLUMN row_num;




--- Exploratory Data Analysis ----
SELECT *
FROM layoffs_staging2;

SELECT MAX(percentage_laid_off)
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;


SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT industry, company, sum(total_laid_off)
FROM layoffs_staging2
GROUP BY industry, company
ORDER BY 3 DESC; 

SELECT country, sum(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC; 

SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

SELECT stage, sum(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

SELECT YEAR(`DATE`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`DATE`)
ORDER BY YEAR(`DATE`) DESC;

SELECT SUBSTRING(`date`, 1, 7) AS `MONTH`, SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC;

-- ROLLING TOTAL --

WITH Rolling_Total AS
(
SELECT SUBSTRING(`date`, 1, 7) AS `MONTH`, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC
)
SELECT `MONTH`, total_off,
SUM(total_off) OVER(ORDER BY `MONTH`) as roll_over_total
FROM Rolling_Total;


select company, YEAR(`DATE`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`DATE`)
ORDER BY 3 DESC;


WITH company_year (company, years, total_laid_off) AS
(
select company, YEAR(`DATE`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`DATE`)
), Company_ranking AS
(SELECT *, dense_rank() OVER( PARTITION BY years ORDER BY total_laid_off DESC) AS RANKING
FROM company_year
WHERE years IS NOT NULL)
SELECT *
FROM Company_ranking
WHERE RANKING <= 5;

