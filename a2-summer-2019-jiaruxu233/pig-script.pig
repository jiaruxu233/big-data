ibig = LOAD 'i-bigrams/' 
USING PigStorage('\t') AS (
gram: chararray, 
year: int, 
occurence: int, 
books: int);

gram_group = GROUP ibig BY gram;
filtered_gram = FOREACH gram_group GENERATE flatten(ibig.gram) AS gram, SUM(ibig.occurence) AS total_occ, SUM(ibig.books) AS total_books, (float)SUM(ibig.occurence)/(float)SUM(ibig.books) AS avg_occurence, MIN(ibig.year) AS first_year, MAX(ibig.year) AS last_year,COUNT(ibig.year) AS count; 
fit_gram_group = FILTER filtered_gram BY (first_year==1950) AND (last_year==2009) AND (count==60);
dist_f_gram = distinct filtered_gram;
fit_gram_group2 = FILTER dist_f_gram BY (first_year==1950) AND (last_year==2009) AND (count==60);
ord_final_gram = ORDER fit_gram_group2 BY avg_occurence DESC;
l_dist_f_gram = limit ord_final_gram 50;
STORE l_dist_f_gram INTO 's3://jiaruxu-bigdatasummer/a2-summer/output' using PigStorage(',') ;

