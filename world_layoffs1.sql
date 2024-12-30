-- World Layoffs Project - Data Cleaning
-- 1. Remove duplicates
-- 2. Standardize the data
-- 3. Null values or blank values
-- 4. Remove any columns or rows

CREATE TABLE layoffs_staging
LIKE layoffs;
-- select * from layoffs_staging;

INSERT layoffs_staging

SELECT * ,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
from layoffs_staging;

WITH duplicate_cte AS
(
SELECT * ,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
from layoffs_staging
)
select *
from duplicate_cte
where row_num > 1;

select *
from layoffs_staging
where company = 'Casper';

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

select *
from layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT * ,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 
`date`, stage, country, funds_raised_millions) AS row_num
from layoffs_staging;

DELETE FROM
layoffs_staging2
where row_num > 1;

-- select * from layoffs_staging2;

-- Standadizing data (finding issues in the data and fix it)
select company, TRIM(company)
from layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

select distinct industry
from layoffs_staging2;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';


select distinct country, trim(trailing '.' from country)
from layoffs_staging2
where country like 'United States%';

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- SELECT DISTINCT `date`
-- FROM layoffs_staging2;

-- UPDATE layoffs_staging2
-- SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- ALTER TABLE layoffs_staging2
-- MODIFY COLUMN `date` DATE;

select * 
from layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company = t2.company
and t1.location = t2.location
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

-- UPDATE layoffs_staging2 t1
-- JOIN layoffs_staging2 t2
-- ON t1.company = t2.company
-- WHERE t1.industry is null
-- AND t2.industry is not null;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL AND t2.industry IS NOT NULL;

-- cannot trust those data below
Delete 
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

select * from layoffs_staging2;


