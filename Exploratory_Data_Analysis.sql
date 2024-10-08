select * from layoffs_staging2;

select max(total_laid_off),max(percentage_laid_off) from layoffs_staging2; 

-- FETCHING ALL THE COMPANY'S DATA WHICH LAID OFF ALL THEIR EMPLOYEES
select * from layoffs_staging2 
where percentage_laid_off = 1
order by total_laid_off desc;

-- FUNDS OF ALL THE COMPANY'S DATA WHICH LAID OFF ALL THEIR EMPLOYEES
select * from layoffs_staging2 
where percentage_laid_off = 1
order by funds_raised_millions desc;

-- TOTAL NUMBER OF EMPLOYEES LAID OFF PER COMPANY
select company,sum(total_laid_off) from layoffs_staging2
group by company
order by 2 desc;

-- TIMELINE
select min(`date`),max(`date`) from layoffs_staging2;

-- TOTAL NUMBER OF EMPLOYEES LAID OFF PER INDUSTRY
select industry,sum(total_laid_off) from layoffs_staging2
group by industry
order by 2 desc;

-- TOTAL NUMBER OF EMPLOYEES LAID OFF PER COUNTRY
select country,sum(total_laid_off) from layoffs_staging2
group by country
order by 2 desc;

-- TOTAL NUMBER OF EMPLOYEES LAID OFF PER YEAR
select YEAR(`DATE`),sum(total_laid_off) from layoffs_staging2
group by 1
order by 2 desc;

-- TOTAL NUMBER OF EMPLOYEES LAID OFF BASED ON THE STAGE OF THE COMPANY
select stage,sum(total_laid_off) from layoffs_staging2
group by 1
order by 2 desc;

-- SUM OF PERCENTAGE_LAID_OFF
select company,sum(percentage_laid_off) from layoffs_staging2
group by 1
order by 2 desc;

select company,avg(percentage_laid_off) from layoffs_staging2
group by 1
order by 2 desc;

-- PERIOD WISE LAID OFF DATA
select substring(`DATE`,1,7) as month, sum(total_laid_off)
from layoffs_staging2
where substring(`DATE`,1,7) is not null
group by 1
order by 1;

-- ROLLING SUM OF THE total_laid_off OVER THE PERIODS
WITH rolling_sum as
(
select substring(`DATE`,1,7) as month, sum(total_laid_off) as sum_total_laid_off
from layoffs_staging2
where substring(`DATE`,1,7) is not null
group by 1
)
select month,sum_total_laid_off
,sum(sum_total_laid_off) over (order by month) as rolling_total_laidoff 
from rolling_sum;

-- SUM OF total_laid_off PER COMPANY PER YEAR
select company,year(`date`) as year, sum(total_laid_off)
from layoffs_staging2
group by 1,2
order by 3 desc;

with per_Company_per_year as 
(
select company,year(`date`) as year, sum(total_laid_off) sum1
from layoffs_staging2
group by 1,2
order by 3 desc
)
select *,dense_rank() over (partition by year order by sum1 desc) as layoff_Rank
from per_Company_per_year
where year is not null
order by layoff_Rank;

-- TOP 5 COMPANY WITH HIGHEST LAYOFF EACH YEAR
with per_Company_per_year as 
(
select company,year(`date`) as year, sum(total_laid_off) sum1
from layoffs_staging2
group by 1,2
order by 3 desc
), company_year_rank as
(
select *,dense_rank() over (partition by year order by sum1 desc) as layoff_Rank
from per_Company_per_year
where year is not null
)
select * from company_year_rank
where layoff_Rank <= 5;

