-- show databases;

-- DATA CLEANING
-- IMPORTED
select * from layoffs;

-- CREATING BACKUP

-- Create table layoffs_staging
-- like layoffs;

-- insert layoffs_staging
-- select * from layoffs;

select count(*) from layoffs_staging;

-- REMOVING DUPES 

with cte1 as (
select *,row_number() over(partition by company,location, industry, total_laid_off, percentage_laid_off,'date',stage,country,funds_raised_millions) as row_num
from layoffs_staging)
select * from cte1
where row_num>1;

select * from layoffs_staging
where company = 'Casper';

-- CAN'T DELETE USING CTE LIKE WE DO IN MSSQL; SO, CREATE A NEW TABLE WITH row_num

with cte1 as (
select *,row_number() over(partition by company,location, industry, total_laid_off, percentage_laid_off,'date',stage,country,funds_raised_millions) as row_num
from layoffs_staging)
delete from cte1
where row_num>1;

-- CREATE TABLE `layoffs_staging2` (
--   `company` text,
--   `location` text,
--   `industry` text,
--   `total_laid_off` int DEFAULT NULL,
--   `percentage_laid_off` text,
--   `date` text,
--   `stage` text,
--   `country` text,
--   `funds_raised_millions` int DEFAULT NULL,
--   `row_num` int
-- ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * from layoffs_staging2;

-- insert into layoffs_staging2
-- select *,row_number() over(partition by company,location, industry, total_laid_off, percentage_laid_off,'date',stage,country,funds_raised_millions) as row_num
-- from layoffs_staging;

select * from layoffs_staging2 where row_num > 1;

-- DISABLE SAFE MODE; Settings -> SQL Editor ->

-- DELETE FROM layoffs_staging2 
-- WHERE row_num > 1;

select count(*) from layoffs_staging2;

-- STANDARDIZING DATA

select distinct(trim(company)) from layoffs_staging2;
select company,trim(company) from layoffs_staging2;

-- THE BELOW UPDATE WILL REPLACE THE BLANKS WITH NULL

-- update layoffs_staging2
-- set company = trim(company); 

select distinct industry,trim(industry) from layoffs_staging2 order by 1;

-- 'Crypto','Crypto Currency','CryptoCurrency' ARE THE SAME. SO, GIVE A COMMON NAME.

select * from layoffs_staging2 where lower(industry) like '%crypto%';
select count(*) from layoffs_staging2 where lower(industry) like '%crypto%';

-- update layoffs_staging2
-- set industry = 'Crypto'
-- where lower(industry) like '%crypto%';

select distinct location, trim(location)
from layoffs_staging2 order by 1;

select distinct country, trim(trailing '.' from country)
from layoffs_staging2 order by 1;

-- update layoffs_staging2
-- set country = trim(trailing '.' from country);

-- DATE NEEDS TO BE IN MM/DD/YYYY

select `date`,str_to_date(`date`,'%m/%d/%Y')
from layoffs_staging2;

select `date`,str_to_date(`date`,'%m/%d/%Y')
from layoffs_staging2;

select 'Feb 3 2000',str_to_date('Feb 3 2000','%M %d %Y');

-- update layoffs_staging2
-- set `date` = str_to_date(`date`,'%m/%d/%Y');

alter table layoffs_staging2
modify column `date` DATE;

-- HANDLE NULL VALUES
select * from layoffs_staging2 
where total_laid_off is NULL
AND percentage_laid_off is NULL;

select industry, count(*) from layoffs_staging2 where industry is null group by industry;

select industry, count(*) from layoffs_staging2 where industry is null or industry = '' group by industry;

select * from layoffs_staging2 where industry is null or industry = '';
-- GET ALL THE COMPANY WHERE INDUSTRY IS NULL OR ''
select company from layoffs_staging2 where industry is null or industry = '';

select * from layoffs_staging2 where company = 'Airbnb'; -- industry = Travel
select * from layoffs_staging2 where company = 'Bally\'s Interactive';
select * from layoffs_staging2 where company = 'Carvana'; -- industry = Transportation
select * from layoffs_staging2 where company = 'Juul'; -- industry = Consumer

select distinct company, industry from layoffs_staging2 t1 
where company in (select company from layoffs_staging2 t2 where industry is null or industry = '')
and industry is not null;

select *
from layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company = t2.company
and t1.location = t2.location
where (t1.industry is NULL or t1.industry = '')
and t2.industry is NOT NULL;

select t1.industry,t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company = t2.company
and t1.location = t2.location
where (t1.industry is NULL or t1.industry = '')
and t2.industry is NOT NULL;

-- update layoffs_staging2 t1
-- join layoffs_staging2 t2
-- on t1.company = t2.company
-- and t1.location = t2.location
-- set t1.industry = t2.industry
-- where (t1.industry is NULL or t1.industry = '')
-- and t2.industry is NOT NULL;

-- TOTAL_LAID_OFF CAN BE POPULATED USING PERCENTAGE_LAID_OFF IF THE TOTAL_NUMBER_OF_EMPLOYEES ARE PROVIDED.

select * from layoffs_staging2 
where total_laid_off is NULL
AND percentage_laid_off is NULL;

-- THE RECORDS FETCHED IN THE ABOVE QUERY HAVE NOT LAID OFF ANY EMPLOYEES.
-- THEY ARE NOT GONNA IMPACT ANYTHING IN THE FINAL DASHBOARD IF THE DASHBOARD IS BASED ON LAID_OFF PERCENTAGE.

-- Delete from layoffs_staging2 
-- where total_laid_off is NULL
-- AND percentage_laid_off is NULL;

-- Alter table layoffs_staging2
-- drop column row_num;

-- *** layoffs_staging2 contains the final cleaned data. ***

-- NEXT STEP: EXPLORATORY DATA ANALYSIS
