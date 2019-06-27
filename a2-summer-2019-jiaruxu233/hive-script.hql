create external table r_bigrams (
gram string,
year int,
occurence int,
book int )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LOCATION '/user/hadoop/r-bigrams';

INSERT OVERWRITE DIRECTORY 's3://jiaruxu-bigdatasummer/a2-summer/hive-result'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
SELECT * FROM (
SELECT gram, sum(occurence), sum(book),sum(occurence)/sum(book) AS avg, min(year) as first, max(year) as last, count(year) as count
FROM r_bigrams
GROUP BY gram
HAVING first == 1950 AND last == 2009 AND count == 60
ORDER BY avg DESC
LIMIT 50) t
ORDER BY t.avg ASC;

