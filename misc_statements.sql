ALTER TABLE raw_lanes ADD COLUMN id SERIAL PRIMARY KEY;
ALTER TABLE raw_lanes_try ADD COLUMN id SERIAL PRIMARY KEY;
alter table pdct_fam_types add column id serial primary key;

select * from raw_lanes limit 100;
select * from raw_lanes_try limit 100;

select "PRODUCT_FAMILY", count(distinct "SHIPMENT_TYPE") as num_types
from raw_lanes 
-- where "PRODUCT_FAMILY" in ('TBA')
group by "PRODUCT_FAMILY"
order by num_types, "PRODUCT_FAMILY" desc;

select distinct "PRODUCT_FAMILY", "SHIPMENT_TYPE", count("SHIPMENT_TYPE") as num_types
from raw_lanes
-- where "PRODUCT_FAMILY" in ('TBA')
group by "PRODUCT_FAMILY", "SHIPMENT_TYPE"
order by "PRODUCT_FAMILY", num_types;

select distinct "PRODUCT_FAMILY" from raw_lanes
where "SHIPMENT_TYPE" in ('DRAY')
order by "PRODUCT_FAMILY";

select "SHIPMENT_TYPE", count("SHIPMENT_TYPE")
from raw_lanes 
group by "SHIPMENT_TYPE";

select "PRODUCT_FAMILY", "LANE", "SHIPMENT_TYPE", 
sum("BILLED_WEIGHT") as total_weight, sum("TOTAL_AMOUNT_PAID") as total_paid
from raw_lanes
where "BILLED_WEIGHT" != 0 
and "PRODUCT_FAMILY" in ('TBA') and "SHIPMENT_TYPE" not in ('OTHER', 'BROKERAGE')
group by "PRODUCT_FAMILY", "LANE", "SHIPMENT_TYPE"
order by "PRODUCT_FAMILY", "SHIPMENT_TYPE";

select distinct "FISCAL_QUARTER_SHIP" from raw_lanes;

select * from "ShipRank";

select "PRODUCT_FAMILY" as prod_fam, 
"SHIP_FROM_NAME" as ori_name, "SHIP_FROM_COUNTRY" as ori_country, "SHIP_FROM_REGION_CODE",
"SHIP_TO_NAME", "SHIP_TO_COUNTRY", "SHIP_TO_REGION_CODE",
"SHIPMENT_TYPE", 
sum("BILLED_WEIGHT") as total_weight, sum("TOTAL_AMOUNT_PAID") as total_paid
from raw_lanes;

select * from "Lanes";

insert into "Lanes" (pdct_fam, ori_name, ori_country, ori_region, desti_name, desti_country, desti_region, ship_type, ship_rank, total_weight, total_paid)
select "PRODUCT_FAMILY", 
lower("SHIP_FROM_NAME"), lower("SHIP_FROM_COUNTRY"), lower("SHIP_FROM_REGION_CODE"),
lower("SHIP_TO_NAME"), lower("SHIP_TO_COUNTRY"), lower("SHIP_TO_REGION_CODE"),
"SHIPMENT_TYPE", ship_rank,
sum("BILLED_WEIGHT"), sum("TOTAL_AMOUNT_PAID")
from raw_lanes rl join "ShipRank" sr 
on rl."SHIPMENT_TYPE" = sr.ship_type
where "BILLED_WEIGHT" != 0 
and "SHIPMENT_TYPE" not in ('OTHER', 'BROKERAGE')
group by "PRODUCT_FAMILY", 
"SHIP_FROM_NAME", "SHIP_FROM_COUNTRY", "SHIP_FROM_REGION_CODE", 
"SHIP_TO_NAME", "SHIP_TO_COUNTRY", "SHIP_TO_REGION_CODE", 
"SHIPMENT_TYPE", ship_rank;
 
select pdct_fam, count(*) as count_pdct_fam 
from "Lanes" 
group by pdct_fam
order by count_pdct_fam desc;

select distinct color from "Lanes";

select * from "Lanes"
where pdct_fam = '4400ISR' -- and pflow = 49 or parent_pflow = 49
order by pflow, path, path_rank
and f > 2;

select * from "Lanes"
where desti_name = ',CN'
and desti_country = 'CN'
and desti_region = 'APAC'
and pdct_fam = '4400ISR';

select * from "Lanes"
where ori_name = 'Pardubice,CZ'
and ori_country = 'CZ'
and ori_region = 'EUM'
and pdct_fam = '3KAGG'

select pdct_fam, count(1) as num from "Lanes"
group by pdct_fam
order by num;


select * from "Lanes" where pdct_fam = 'IE2000'

select * from pdct_fam_types where pdct_fam = 'AIRANT';

select distinct "orgType", "largestPF" from pdct_fam_types_raw;


create table pdct_fam_types as
select distinct "largestPF" as pdct_fam, "orgType" as pdct_type
from pdct_fam_types_raw;

select distinct "SHIP_FROM_COUNTRY" from raw_lanes;


insert into "Nodes" (name, country, region, role, pdct_fam)
select distinct ori_name, ori_country, ori_region, ori_role, pdct_fam
from "Lanes"
where in_pflow = 1;

insert into "Nodes" (name, country, region, role, pdct_fam)
select distinct desti_name, desti_country, desti_region, desti_role, pdct_fam
from "Lanes"
where in_pflow = 1 and desti_role = 'Customer';

alter table "Nodes" add column node_id serial primary key; 

select * from "Nodes" where role not in ('Customer')

update "Nodes" 
            set capacity = subquery.cap
            from (
                select total_alpha * 100 as cap
                from "Lanes" join "Nodes"
                on ori_role = role and ori_name = name and ori_country = country and ori_region = region
                where ori_role in ('PCBA', 'DF', 'GHUB', 'OSLC', 'DSLC')
                and pflow = 1
            ) as subquery


alter table "Edges" add column edge_id serial primary key; 

insert into "Edges" 
(pdct_fam, ori_name, ori_country, ori_region, ori_role, 
desti_name, desti_country, desti_region, desti_role, 
transport_mode, transport_distance)
(select pdct_fam, ori_name, ori_country, ori_region, ori_role,
 desti_name, desti_country, desti_region, desti_role
 floor(rand(200-50) * 50)
from Lanes)


alter table "Locations" add column location_id serial primary key;

insert into "Locations" (name, country, region)
select distinct name, country, region
from "Nodes";



