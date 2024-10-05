# Data-cleaning-and-standization
select *
from layoffs;

create table layoffs_staging
like layoffs;


insert layoffs_staging
select*
from layoffs;


select*,
row_number() over(
partition by company, industry, total_laid_off,percentage_laid_off, `date`) as row_num
from layoffs_staging;


with duplicate_cte as
(
select*,
row_number() over(
partition by company, industry, total_laid_off,percentage_laid_off, `date`) as row_num
from layoffs_staging
)
select*
from duplicate_cte
where row_num > 1;

select*
from layoffs_staging
where conpany = 'oda';


with duplicate_cte as
(
select*,
row_number() over(
partition by company, industry, total_laid_off,percentage_laid_off, `date`) as row_num
from layoffs_staging
)
delete
from duplicate_cte
where row_num > 1;




CREATE TABLE `layoffs_staging2` (
`company` text,
`location`text,
`industry`text,
`total_laid_off` INT,
`percentage_laid_off` text,
`date` text,
`stage`text,
`country` text,
`funds_raised_millions` int,
row_num INT
);

select*
from layoffs_staging2
where row_num > 1;

INSERT INTO layoffs_staging2
select *,
row_number() over(
partition by company, industry, total_laid_off,percentage_laid_off, `date`, 
stage, country,funds_raised_millions) as row_num
from layoffs_staging;

delete
from layoffs_staging2
where row_num > 1;


select *
from layoffs_staging2;


-- standardizing data

SELECT company, trim(company)
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);


SELECT distinct industry
from layoffs_staging2 ;

update layoffs_staging2
set industry ='Crypto'
where industry like 'Crypto%';


SELECT distinct country, trim(trailing '.' from country)
from layoffs_staging2 
order by 1;

update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United States%';

SELECT `date`
from layoffs_staging2 ;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

alter table layoffs_staging2
modify column `date` date;

SELECT *
from layoffs_staging2
where total_laid_off is null 
and percentage_laid_off is null;


update layoffs_staging2
set industry = null
where industry = '';

SELECT *
from layoffs_staging2
where industry is null
or  industry = '';

SELECT *
from layoffs_staging2
where company like 'Bally%';


select t1.industry,t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company = t2.company
and t1.location = t2.location
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;



update layoffs_staging2 t1
join layoffs_staging2 t2
on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null 
and t2.industry is not null;


SELECT *
from layoffs_staging2;



SELECT *
from layoffs_staging2
where total_laid_off is null 
and percentage_laid_off is null;


delete
from layoffs_staging2
where total_laid_off is null 
and percentage_laid_off is null;


SELECT *
from layoffs_staging2;


alter table layoffs_staging2
drop column row_num;
