create database wiki_db;

show databases;

use wiki_db;

SET hive.exec.dynamic.partition = true;

SET hive.exec.dynamic.partition.mode = nonstrict;
---------------------Question1------------------------
CREATE TABLE pageviews_jan20(
	domain_code String,
	page_title String,
	count_views INT,
	total_response_size INT)
	ROW FORMAT DELIMITED
	FIELDS TERMINATED BY ' ';

--Drop Table pageviews_jan20;

LOAD DATA LOCAL INPATH '/home/tyler/jan20' INTO TABLE pageviews_jan20;

SELECT * from pageviews_jan20;

Create table pv_jan20_en_tot as
SELECT page_title, sum(count_views) as cv from pageviews_jan20 
where domain_code LIKE 'en%' group by page_title order by cv DESC;

drop table pv_jan20_en_tot;

SELECT * from pv_jan20_en_tot;
------------------------------------------------Question 2---------------------------------------
CREATE TABLE pageviews_dec1(
	domain_code String,
	page_title String,
	count_views INT,
	total_response_size INT)
	ROW FORMAT DELIMITED
	FIELDS TERMINATED BY ' ';

LOAD DATA LOCAL INPATH '/home/tyler/dec01' INTO TABLE pageviews_dec1;

CREATE TABLE pageviews_dec10(
	domain_code String,
	page_title String,
	count_views INT,
	total_response_size INT)
	ROW FORMAT DELIMITED
	FIELDS TERMINATED BY ' ';

LOAD DATA LOCAL INPATH '/home/tyler/dec10' INTO TABLE pageviews_dec10;

CREATE TABLE pageviews_dec20(
	domain_code String,
	page_title String,
	count_views INT,
	total_response_size INT)
	ROW FORMAT DELIMITED
	FIELDS TERMINATED BY ' ';

LOAD DATA LOCAL INPATH '/home/tyler/dec20' INTO TABLE pageviews_dec20;

CREATE TABLE pageviews_dec30(
	domain_code String,
	page_title String,
	count_views INT,
	total_response_size INT)
	ROW FORMAT DELIMITED
	FIELDS TERMINATED BY ' ';

LOAD DATA LOCAL INPATH '/home/tyler/dec30' INTO TABLE pageviews_dec30;

CREATE table pv_dec1_en_tot as
SELECT page_title, SUM(count_views) as count_views from pageviews_dec1 
where domain_code Like 'en%' GROUP by page_title order by count_views DESC;

CREATE table pv_dec10_en_tot as
SELECT page_title, SUM(count_views) as count_views from pageviews_dec10 
where domain_code Like 'en%' GROUP by page_title order by count_views DESC;

CREATE table pv_dec20_en_tot as
SELECT page_title, SUM(count_views) as count_views from pageviews_dec20 
where domain_code Like 'en%' GROUP by page_title order by count_views DESC;

CREATE table pv_dec30_en_tot as
SELECT page_title, SUM(count_views) as count_views from pageviews_dec30 
where domain_code Like 'en%' GROUP by page_title order by count_views DESC;

--drop table pv_dec30_en_tot;

CREATE table pv_dec_avg as
Select page_title, Round(AVG(count_views),0) as count_views FROM (
	Select page_title, count_views from pv_dec1_en_tot union all
	Select page_title, count_views from pv_dec10_en_tot union all
	Select page_title, count_views from pv_dec20_en_tot union all
	Select page_title, count_views from pv_dec30_en_tot)x group by page_title order by count_views DESC;

SELECT * from pv_dec_avg;

drop table pv_dec_avg;
-------------------------------------------------------------------------make clickstream------------------------------------------------------
CREATE TABLE clickStream(
	prev String,
	curr String,
	ref_type String,
	occ INT)
	ROW FORMAT DELIMITED
	FIELDS TERMINATED BY '\t';

LOAD DATA LOCAL INPATH '/home/tyler/clickstream-enwiki-2020-12.tsv.gz' INTO TABLE ClickStream;

SELECT * FROM clickstream order by occ ASC;

Drop Table clickstream;

Create table clickstream_link as
Select prev, curr, occ from clickstream where ref_type = 'link';

drop table clickstream_link; 

Create table link_frac_per_pair as
Select c.prev, c.curr, c.occ as link_clicked, p.count_views * 31 as total_view_count, 
Round((c.occ / (p.count_views * 31))*100,4) as link_percent
from pv_dec_avg as p
inner join clickstream_link as c
on c.prev = p.page_title;

drop table link_frac_per_pair;

Create table link_frac_tot as
SELECT prev, SUM(link_clicked) as tot_links_clicked, total_view_count, 
Round((SUM(link_clicked) / total_view_count)*100,4) as link_percent
from link_frac_per_pair 
group by prev,total_view_count 
order by link_percent DESC;

drop table link_frac_tot;

SELECT * from link_frac_tot where total_view_count > 1000000;
--------------------------------------Question3-------------------------------------------------
Create table hc_series as
Select f.prev as first_site, s.prev as second_site, s.curr as third_site, f.link_clicked as first_page_links_clicked,
Round((s.link_percent/100) * f.link_clicked,0) as third_site_views, 
Round((((s.link_percent/100) * f.link_clicked)/ f.link_clicked)*100,2) as reatined_viewers_percentage from
(SELECT * from link_frac_per_pair where prev = 'Hotel_California') as f 
inner join (Select * from link_frac_per_pair where prev in (Select curr from clickstream_link where prev = 'Hotel_California')) as s
on f.curr = s.prev;

drop table hc_series;

Create view question3 as
Select * from hc_series 
where reatined_viewers_percentage < 100 and
first_page_links_clicked > 1000
order by reatined_viewers_percentage DESC;

drop view question3;

SELECT * from question3;
-----------------------------------Question 4-----------------------------------------------
CREATE TABLE pv_us_hours(
	domain_code String,
	page_title String,
	count_views INT,
	total_response_size INT)
	ROW FORMAT DELIMITED
	FIELDS TERMINATED BY ' ';

drop table pv_us_hours;

LOAD DATA LOCAL INPATH '/home/tyler/dec01/pageviews-20201201-010000.gz' INTO TABLE pv_us_hours;
LOAD DATA LOCAL INPATH '/home/tyler/dec01/pageviews-20201201-020000.gz' INTO TABLE pv_us_hours;
LOAD DATA LOCAL INPATH '/home/tyler/dec01/pageviews-20201201-030000.gz' INTO TABLE pv_us_hours;

CREATE TABLE pv_world_hours(
	domain_code String,
	page_title String,
	count_views INT,
	total_response_size INT)
	ROW FORMAT DELIMITED
	FIELDS TERMINATED BY ' ';

drop table pv_world_hours;

LOAD DATA LOCAL INPATH '/home/tyler/dec01/pageviews-20201201-080000.gz' INTO TABLE pv_world_hours;
LOAD DATA LOCAL INPATH '/home/tyler/dec01/pageviews-20201201-090000.gz' INTO TABLE pv_world_hours;
LOAD DATA LOCAL INPATH '/home/tyler/dec01/pageviews-20201201-100000.gz' INTO TABLE pv_world_hours;

CREATE view pv_us_tot as
Select page_title, SUM(count_views) as count_views from pv_us_hours where domain_code like 'en%' group by page_title order by count_views DESC;

SELECT * FROM pv_us_tot;

Create view pv_world_tot as
Select page_title, SUM(count_views) as count_views from pv_world_hours where domain_code like 'en%' group by page_title order by count_views DESC;

SELECT * FROM pv_us_tot;

CREATE table us_vs_ww as
Select u.page_title, u.count_views as us_views, w.count_views as world_views, Round((u.count_views/w.count_views)*100,2) as comparison_percentage
from pv_world_tot as w 
inner join pv_us_tot as u 
on w.page_title = u.page_title
order by comparison_percentage DESC;

SELECT * from us_vs_ww where us_views > 10000;
--------------------------------------------------------------------------------Question 5---------------------------------------------------------------
CREATE TABLE revision_wiki (
	wiki_db STRING,
	event_entity STRING,
	event_type STRING,
	event_timestamp STRING,
	event_comment STRING,
	event_user_id INT,
	event_user_text_historical STRING,
	event_user_text STRING,
	event_user_blocks_historical STRING,
	event_user_blocks STRING,
	event_user_groups_historical STRING,
	event_user_groups STRING,
	event_user_is_bot_by_historical STRING,
	event_user_is_bot_by STRING,
	event_user_is_created_by_self BOOLEAN,
	event_user_is_created_by_system BOOLEAN,
	event_user_is_created_by_peer BOOLEAN,
	event_user_is_anonymous BOOLEAN, 
	event_user_registration_timestamp STRING,
	event_user_creation_timestamp STRING,
	event_user_first_edit_timestamp STRING,
	event_user_revision_count INT,
	event_user_seconds_since_previous_revision INT,
	page_id INT,
	page_title_historical  STRING,
	page_title  STRING,
	page_namespace_historical INT,
	page_namespace_is_content_historical BOOLEAN,
	page_namespace INT,
	page_namespace_is_content BOOLEAN,
	page_is_redirect BOOLEAN,
	page_is_deleted BOOLEAN,
	page_creation_timestamp STRING,
	page_first_edit_timestamp STRING,
	page_revision_count INT,
	page_seconds_since_previous_revision INT,
	user_id INT,
	user_text_historical string,	
	user_text	string,
	user_blocks_historical string,
	user_blocks	string,	
	user_groups_historical	string,	
	user_groups	string,
	user_is_bot_by_historical string,	
	user_is_bot_by	string,	
	user_is_created_by_self boolean,	
	user_is_created_by_system boolean,
	user_is_created_by_peer boolean,
	user_is_anonymous boolean,
	user_registration_timestamp	string,
	user_creation_timestamp	string,
	user_first_edit_timestamp	string,
	revision_id INT,
	revision_parent_id INT, 
	revision_minor_edit boolean, 
	revision_deleted_parts	string,
	revision_deleted_parts_are_suppressed boolean,
	revision_text_bytes INT, 
	revision_text_bytes_diff INT, 
	revision_text_sha1	string,
	revision_content_model	string, 
	revision_content_format	string, 
	revision_is_deleted_by_page_deletion boolean,	
	revision_deleted_by_page_deletion_timestamp	string, 
	revision_is_identity_reverted boolean,
	revision_first_identity_reverting_revision_id INT,
	revision_seconds_to_identity_revert INT,
	revision_is_identity_revert boolean,
	revision_is_from_before_page_creation boolean,
	revision_tags	string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';
SELECT * from revision_wiki;
drop table revision_wiki;

LOAD DATA LOCAL INPATH '/home/tyler/2020-12.enwiki.2020-12.tsv.bz2' INTO TABLE revision_wiki;

Create view vandalized_pages as
Select * from revision_wiki where revision_is_identity_revert = true and revision_minor_edit = false;

Create table time_till_revision as
SELECT p.page_title as page_title, round(count_views / 1440,0) views_per_min, v.revision_seconds_to_identity_revert / 60 as min_til_revision,  
Round((v.revision_seconds_to_identity_revert / 60) * (count_views / 1440),0) as vandalized_page_views
from pv_dec_avg as p 
inner join vandalized_pages as v on p.page_title = v.page_title;

SELECT * from time_till_revision order by vandalized_page_views DESC;

drop table time_till_revision;

Select Round(AVG(vandalized_page_views),0) as avg_views_of_vandalized_page from time_till_revision;
-----------------------------------Question 6----------------------------------------------------
Create table most_distinct_links_clicked as
SELECT prev, COUNT(prev) as number_of_distinct_links
from clickstream_link 
group by prev 
order by number_of_distinct_links DESC;

SELECT * from most_distinct_links_clicked;
	
