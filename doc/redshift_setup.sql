-- ------------------------------------------------------------------------------------
-- A SQL script for create tables in Redshift cluster
-- Redshift Analytic Functions and Aggregate query are used for data analysis 
-- ------------------------------------------------------------------------------------

#create table schma with distkey and sortkey
create table phamacy(
id integer not null,
record_id integer not null,
date_of_service timestamp,
drug_class varchar,
drug_title varchar,
therapeutic_category varchar,
primary key(id))
sortkey(record_id);

# check information for table
select "column", type, encoding, distkey, sortkey
from pg_table_def where tablename = 'pharmacy';

# alter column type for table
alter table pharmacy rename to pharmacy_Temp;
 
create table pharmacy as
SELECT adj_pharmacy_charges
       , cast(date_of_service as datetime)
       , drug_class 
       , generic_drug
       , pharmacy_charges
       , cast(record_id as int8)
       , therapeutic_category
 FROM pharmacy_Temp;
 
 drop table pharmacy_Temp;

# aggragation function to get infection records for each patient
create table agg_infection as 
  select record_id,
      listagg(bacteria,',') within group (order by otu desc) as all_bacteria,
      listagg(otu,',') within group (order by otu desc) as all_otu
  from infection
  group by record_id;