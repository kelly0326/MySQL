-- World Layoffs Project - Data Cleaning
-- 1. Remove duplicates
-- 2. Standardize the data
-- 3. Null values or blank values
-- 4. Remove any columns or rows

-- 1. Remove duplicates

# Create a duplicate table from layoffs (raw date) -- layoffs_statging
CREATE TABLE layoffs_staging
LIKE layoffs;
INSERT layoffs_staging

-- select * from layoffs_staging;

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

-- select * from layoffs_staging2;

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

-- 3. Evaluate NULL values
-- The NULL values in total_laid_off, percentage_laid_off, and funds_raised_millions appear valid.
-- Keeping them as NULL is preferred since it simplifies calculations during the EDA phase.
-- Therefore, no changes are needed for these NULL values.


-- 4. Remove any columns and rows as needed

-- Retrieves rows where industry is missing or empty in t1 but non-null in t2 for the same company and location
-- SELECT * 
-- FROM layoffs_staging2 t1
-- JOIN layoffs_staging2 t2
-- ON t1.company = t2.company
-- AND t1.location = t2.location
-- WHERE (t1.industry IS NULL OR t1.industry = '')
-- AND t2.industry IS NOT NULL;


-- Updates industry in t1 with the value from t2 where industry is missing (NULL) in t1 and non-null in t2 for matching company
-- UPDATE layoffs_staging2 t1
-- JOIN layoffs_staging2 t2
-- ON t1.company = t2.company
-- SET t1.industry = t2.industry
-- WHERE t1.industry IS NULL AND t2.industry IS NOT NULL;

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


