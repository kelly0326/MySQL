-- World Layoffs Project

-- Data Cleaning
-- 1. Remove duplicates
-- 2. Standardize the data
-- 3. Null values or blank values
-- 4. Remove any columns or rows

-- 1. Remove duplicates
select * from layoffs;

# Create a duplicate table from layoffs (raw data) -- layoffs_statging
CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT layoffs_staging
SELECT *
FROM layoffs;

select * from layoffs_staging;

-- Create a Common Table Expression (CTE) to identify duplicate rows;  Select rows where row_num > 1, indicating duplicates
WITH duplicate_cte AS
(
SELECT * ,
ROW_NUMBER() OVER(
PARTITION BY company, location, 
industry, total_laid_off, percentage_laid_off, `date`, stage, 
country, funds_raised_millions) AS row_num
from layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- Create an empty table - layoffs_staging2
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

-- select * from layoffs_staging2;

-- Insert data into the new table with row numbers
INSERT INTO layoffs_staging2
SELECT * ,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, 
total_laid_off, percentage_laid_off, 
`date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

# Delete the duplicate rows from the new table
DELETE FROM
layoffs_staging2
WHERE row_num > 1; -- This removes duplicate rows identified by row_num > 1 from the layoffs_staging2 table

select * from layoffs_staging2;

-----------------------------------------------------------------------------------------------------------------------------------
-- 2. Standadize data (finding issues in the data and fix it)

-- COMPANY
SELECT company, TRIM(company)
FROM layoffs_staging2;

-- Clean whitespace from the 'company' column
UPDATE layoffs_staging2
SET company = TRIM(company); -- TRIM() removes any leading or trailing whitespace from the 'company' column to ensure data consistency

-- INDUSTRY
-- Standardize industry names starting with 'Crypto'
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%'; -- This query standardizes all industry values starting with 'Crypto' to ensure consistency in naming

-- Verify cleaned 'industry' values
SELECT DISTINCT industry
FROM layoffs_staging2;

-- COUNTRY
-- Clean trailing periods from 'country' column for 'United States'
UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%'; -- This query removes any trailing periods from 'country' values that start with 'United States' to ensure consistency.

-- Verify cleaned 'country' values
SELECT DISTINCT country, trim(TRAILING '.' FROM country)
FROM layoffs_staging2
WHERE country LIKE 'United States%';

-- DATE
-- Standardize date format
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Modify the `date` column type
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE; -- This alters the `date` column to enforce a DATE data type

-- Verify updated `date` column
SELECT `date`
FROM layoffs_staging2;

-- industry
-- Check if there is any null/empty rows in the industry column
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY industry;

-- Fetches rows where industry is NULL or empty, sorted by industry
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'airbnb%';

-- Replaces empty industry values with NULL
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';
 
-- Finds rows with industry as NULL or empty, sorted by industry
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- Fills NULL industry values in t1 using matching industry values from t2 based on the same company
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- Fetches rows where industry is NULL or empty, sorted by industry
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

select * from layoffs_staging2 where company = 'Airbnb';

-----------------------------------------------------------------------------------------------------------------------------------
-- 3. Evaluate NULL values or blank values
-- The NULL values in total_laid_off, percentage_laid_off, and funds_raised_millions appear valid.
-- Keeping them as NULL is preferred since it simplifies calculations during the EDA phase.
-- Therefore, no changes are needed for these NULL values.

-----------------------------------------------------------------------------------------------------------------------------------
-- 4. Remove any columns and rows as needed

-- Retrieves rows from layoffs_staging2 where both total_laid_off and percentage_laid_off are NULL
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Updates the industry column to NULL where its current value is an empty string
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

-- Delete useless data
DELETE 
FROM layoffs_staging2
WHERE total_laid_off is null
AND percentage_laid_off is null;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- Check the final data table after cleaning completed
SELECT * FROM layoffs_staging2;


-----------------------------------------------------------------------------------------------------------------------------------
-- Exploratory Data Analysis (EDA)

-- Retrieve all columns from companies where 100% of employees were laid off
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

-- Displays the total number of layoffs per company, sorted by the highest total layoffs
SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

-- Retrieves the earliest and latest dates from the date column in the dataset
SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

-- Displays the total number of layoffs per industry
SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

-- Displays the total number of layoffs by countries
SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

-- Displays the total number of layoffs by years
SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

-- Displays the total number of layoffs by stage
SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

-- Displays the total number of laidoffs per month
SELECT SUBSTRING(`date`,1,7) AS `MONTH`, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC;


-- Ranks companies by total layoffs per year and retrieves the top 3 for each year, ordered chronologically and by layoffs
WITH Company_Year AS 
(
  SELECT company, YEAR(date) AS years, SUM(total_laid_off) AS total_laid_off
  FROM layoffs_staging2
  GROUP BY company, YEAR(date)
)
, Company_Year_Rank AS (
  SELECT company, years, total_laid_off, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
  FROM Company_Year
)
SELECT company, years, total_laid_off, ranking
FROM Company_Year_Rank
WHERE ranking <= 3
AND years IS NOT NULL
ORDER BY years ASC, total_laid_off DESC;

