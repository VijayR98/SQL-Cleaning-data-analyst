-- DATA CLEANING

-- 1. REMOVE DUPILCATES
-- 2. STANDARIZE THE DATA
-- 3. NULL VALUES OR BLANK VALUES
-- 4. REMOVE ANY COLUMNS

select * from layoffs;


                                 -- COPY RAW DATA TO OTHER TABLE

create table layoffs_staging
like layoffs;

select * from layoffs_staging;

insert layoffs_staging
select * from layoffs;


                              -- 1. REMOVE DUPLICATES , for that create row_num using window function

select *,
Row_number() 
over(partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,fund_raised_millions) as row_num
from layoffs_staging;


                       --  CREATE CTE and add above statement, shows duplicate 

WITH duplicate_CTE AS
(
select *,
Row_number() 
over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
select * from duplicate_CTE
where row_num>1;


                               -- FOR CHECKING

select * from
layoffs_staging
where company = 'casper' ;


                             -- This not works for delete duplicate
                             
WITH duplicate_CTE AS
(
select *,
Row_number() 
over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging
)
Delete from duplicate_CTE
where row_num>1;

                           
                           -- For delete duplicates create other table ,this layoffs_staging right click copy to clipboard and click create 
						    --  table then paste change staging2 and add column row_num then insert values and delete
                            
                           
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
  `Row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
                          

select * from
layoffs_staging2; 


insert into layoffs_staging2
select *,
Row_number() 
over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging;
                          

select * from
layoffs_staging2
where row_num>1;

                       -- DELETE DUPLICATES
                       
delete from
layoffs_staging2
where row_num>1;

select * from
layoffs_staging2;


-- 2. STANDARDIZING DATA (finding issues and fix it)

						-- company

select distinct company from 
layoffs_staging2;

update layoffs_staging2
set company = trim(company);


							-- industry

select distinct industry from 
layoffs_staging2
order by 1;

select * from 
layoffs_staging2
where industry like 'crypto%';


update layoffs_staging2
set industry = 'crypto'
where industry like 'crypto%';


-- location

select distinct location from 
layoffs_staging2
order by 1;


								-- country

select distinct country from 
layoffs_staging2
order by 1;

								-- united states has two with (.) at end

select * from 
layoffs_staging2
where country like 'united states%';


									-- trailing removes at end


select distinct country , trim(trailing '.' from country) 
from layoffs_staging2
order by 1;


update layoffs_staging2
set country =  trim(trailing '.' from country)
where country like 'united states%';

								
                                -- date
								-- change string to date type

select `date`,
str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2;

								
update layoffs_staging2
set date =  str_to_date(`date`, '%m/%d/%Y');


										-- still text format in column to change date type alter use

alter table layoffs_staging2
modify column `date` date;



-- 3. NULL VALUES AND BLANK SPACE


select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;


select *
from layoffs_staging2
where industry is null
or industry = '';

						
                            --   using company, industry previous joins to null and blank 

select *
from layoffs_staging2
where company = 'airbnb';

								-- Change blank space to NULL

Update layoffs_staging2
set industry = null
where industry = '';

									-- Using joins industry null values change to previous indutry

select t1.industry, t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
	ON t1.company = t2.company
where t1.industry is null 
AND t2.industry is not null;


                                   -- Update joins

Update layoffs_staging2 t1
join layoffs_staging2 t2
	ON t1.company = t2.company
set t1.industry = t2.industry    
where t1.industry is null 
AND t2.industry is not null;


										-- Delete 2 columns null values

Delete
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;


										-- 4. REMOVE ANY COLUMNS

Alter table layoffs_staging2
Drop column Row_num; 

select * from layoffs_staging2;

                                