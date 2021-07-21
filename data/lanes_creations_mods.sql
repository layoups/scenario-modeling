select distinct "FISCAL_QUARTER_SHIP" from "SCDS_DB"."SCDS_SCDSI_STG"."SCDSI_CV_LANE_RATE_AUTOMATION_PL"

create table "SCDS_DB"."SCDS_SCDSI_STG"."SCDSI_SHIP_RANK" (
  SHIP_TYPE_ID integer AUTOINCREMENT(1,1) primary key,
  SHIP_TYPE varchar(20),
  SHIP_RANK varchar(20)
);

drop table "SCDS_DB"."SCDS_SCDSI_STG"."SCDSI_SHIP_RANK";

insert into "SCDS_DB"."SCDS_SCDSI_STG"."SCDSI_SHIP_RANK" (SHIP_TYPE, SHIP_RANK)
values ('PO', 0);

insert into  (BASELINE_ID, DATE, DESCRIPTION)
values (1, current_date(), 'trial')


select * from "SCDS_DB"."SCDS_SCDSI_STG"."SCDSI_SHIP_RANK";

alter table "SCDS_DB"."SCDS_SCDSI_STG"."SCDSI_CV_LANE_RATE_AUTOMATION_PL" add column row_id integer;


show parameters like 'network_policy' in user SCDS_SCDSI_ETL_SVC;


create table "SCDS_DB"."SCDS_SCDSI_WI"."SCDSI_LOCATIONS" (
  LOCATION_ID integer AUTOINCREMENT(1,1) primary key,
  LAT float,
  LONG float
);

alter table "SCDS_DB"."SCDS_SCDSI_STG"."SCDSI_ALTERNATIVE_NODES" drop column "LOCATION_COUNTRY_REGION";

########################################## CHANGE LOCATION NAMING #####################################################


##################### Nodes ######################

alter table "SCDS_DB"."SCDS_SCDSI_WI"."SCDSI_SCENARIO_NODES" add column "NAME" VARCHAR(50);
alter table "SCDS_DB"."SCDS_SCDSI_WI"."SCDSI_SCENARIO_NODES" add column "COUNTRY" VARCHAR(50);
alter table "SCDS_DB"."SCDS_SCDSI_WI"."SCDSI_SCENARIO_NODES" add column "REGION" VARCHAR(50);

alter table "SCDS_DB"."SCDS_SCDSI_WI"."SCDSI_SCENARIO_NODES" drop column "LOCATION_COUNTRY_REGION";

alter table "SCDS_DB"."SCDS_SCDSI_STG"."SCDSI_ALTERNATIVE_NODES" add column "ALT_NAME" VARCHAR(50);
alter table "SCDS_DB"."SCDS_SCDSI_STG"."SCDSI_ALTERNATIVE_NODES" add column "ALT_COUNTRY" VARCHAR(50);
alter table "SCDS_DB"."SCDS_SCDSI_STG"."SCDSI_ALTERNATIVE_NODES" add column "ALT_REGION" VARCHAR(50);

alter table "SCDS_DB"."SCDS_SCDSI_WI"."SCDSI_SCENARIO_EDGES" drop column "ORI_LOCATION_COUNTRY_REGION";
alter table "SCDS_DB"."SCDS_SCDSI_WI"."SCDSI_SCENARIO_EDGES" drop column "DESTI_LOCATION_COUNTRY_REGION";

alter table "SCDS_DB"."SCDS_SCDSI_WI"."SCDSI_SCENARIO_EDGES" add column "ORI_NAME" VARCHAR(50);
alter table "SCDS_DB"."SCDS_SCDSI_WI"."SCDSI_SCENARIO_EDGES" add column "ORI_COUNTRY" VARCHAR(50);
alter table "SCDS_DB"."SCDS_SCDSI_WI"."SCDSI_SCENARIO_EDGES" add column "ORI_REGION" VARCHAR(50);

alter table "SCDS_DB"."SCDS_SCDSI_WI"."SCDSI_SCENARIO_EDGES" add column "DESTI_NAME" VARCHAR(50);
alter table "SCDS_DB"."SCDS_SCDSI_WI"."SCDSI_SCENARIO_EDGES" add column "DESTI_COUNTRY" VARCHAR(50);
alter table "SCDS_DB"."SCDS_SCDSI_WI"."SCDSI_SCENARIO_EDGES" add column "DESTI_REGION" VARCHAR(50);


##################### Baselines #########################

alter table "SCDS_DB"."SCDS_SCDSI_STG"."SCDSI_BASELINES" add column "START" VARCHAR(16777216);
alter table "SCDS_DB"."SCDS_SCDSI_STG"."SCDSI_BASELINES" add column "END" VARCHAR(16777216);

##################### Locations #########################

alter table "SCDS_DB"."SCDS_SCDSI_WI"."SCDSI_LOCATIONS" add column "NAME" VARCHAR(50);
alter table "SCDS_DB"."SCDS_SCDSI_WI"."SCDSI_LOCATIONS" add column "COUNTRY" VARCHAR(50);
alter table "SCDS_DB"."SCDS_SCDSI_WI"."SCDSI_LOCATIONS" add column "REGION" VARCHAR(50);

##################### Totals ##############################

alter table "SCDS_DB"."SCDS_SCDSI_WI"."SCDSI_SCENARIO_LANES" drop column "AGG_TOTAL_AMOUNT_PAID";
alter table "SCDS_DB"."SCDS_SCDSI_WI"."SCDSI_SCENARIO_LANES" drop column "AGG_CHARGEABLE_WEIGHT_TOTAL_AMOUNT";

alter table "SCDS_DB"."SCDS_SCDSI_WI"."SCDSI_SCENARIO_LANES" add column "TOTAL_PAID" float;
alter table "SCDS_DB"."SCDS_SCDSI_WI"."SCDSI_SCENARIO_LANES" add column "TOTAL_WEIGHT" float;



