# Databricks notebook source


# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

#creating user defined schema for all 6 files 
# Define schemas
#file-1
charges_schema = StructType([
    StructField("CRASH_ID", StringType(), True),
    StructField("UNIT_NBR", IntegerType(), True),
    StructField("PRSN_NBR", IntegerType(), True),
    StructField("CHARGE", StringType(), True),
    StructField("CITATION_NBR", StringType(), True)
])
#file-2
damages_schema = StructType([
    StructField("CRASH_ID", StringType(), True),
    StructField("DAMAGED_PROPERTY", StringType(), True)
])
#file-3
endorsements_schema = StructType([
    StructField("CRASH_ID", StringType(), True),
    StructField("UNIT_NBR", IntegerType(), True),
    StructField("DRVR_LIC_ENDORS_ID", StringType(), True)
])
#file-4
primary_person_schema = StructType([
    StructField("CRASH_ID", StringType(), True),
    StructField("UNIT_NBR", IntegerType(), True),
    StructField("PRSN_NBR", IntegerType(), True),
    StructField("PRSN_TYPE_ID", StringType(), True),
    StructField("PRSN_OCCPNT_POS_ID", StringType(), True),
    StructField("PRSN_INJRY_SEV_ID", StringType(), True),
    StructField("PRSN_AGE", IntegerType(), True),
    StructField("PRSN_ETHNICITY_ID", StringType(), True),
    StructField("PRSN_GNDR_ID", StringType(), True),
    StructField("PRSN_EJCT_ID", StringType(), True),
    StructField("PRSN_REST_ID", StringType(), True),
    StructField("PRSN_AIRBAG_ID", StringType(), True),
    StructField("PRSN_HELMET_ID", StringType(), True),
    StructField("PRSN_SOL_FL", StringType(), True),
    StructField("PRSN_ALC_SPEC_TYPE_ID", StringType(), True),
    StructField("PRSN_ALC_RSLT_ID", StringType(), True),
    StructField("PRSN_BAC_TEST_RSLT", FloatType(), True),
    StructField("PRSN_DRG_SPEC_TYPE_ID", StringType(), True),
    StructField("PRSN_DRG_RSLT_ID", StringType(), True),
    StructField("DRVR_DRG_CAT_1_ID", StringType(), True),
    StructField("PRSN_DEATH_TIME", StringType(), True),
    StructField("INCAP_INJRY_CNT", IntegerType(), True),
    StructField("NONINCAP_INJRY_CNT", IntegerType(), True),
    StructField("POSS_INJRY_CNT", IntegerType(), True),
    StructField("NON_INJRY_CNT", IntegerType(), True),
    StructField("UNKN_INJRY_CNT", IntegerType(), True),
    StructField("TOT_INJRY_CNT", IntegerType(), True),
    StructField("DEATH_CNT", IntegerType(), True),
    StructField("DRVR_LIC_TYPE_ID", StringType(), True),
    StructField("DRVR_LIC_STATE_ID", StringType(), True),
    StructField("DRVR_LIC_CLS_ID", StringType(), True),
    StructField("DRVR_ZIP", StringType(), True)
])
#file-5
restrict_schema = StructType([
    StructField("CRASH_ID", StringType(), True),
    StructField("UNIT_NBR", IntegerType(), True),
    StructField("DRVR_LIC_RESTRIC_ID", StringType(), True)
])
#file-6
units_schema = StructType([
    StructField("CRASH_ID", StringType(), True),
    StructField("UNIT_NBR", IntegerType(), True),
    StructField("UNIT_DESC_ID", StringType(), True),
    StructField("VEH_PARKED_FL", StringType(), True),
    StructField("VEH_HNR_FL", StringType(), True),
    StructField("VEH_LIC_STATE_ID", StringType(), True),
    StructField("VIN", StringType(), True),
    StructField("VEH_MOD_YEAR", IntegerType(), True),
    StructField("VEH_COLOR_ID", StringType(), True),
    StructField("VEH_MAKE_ID", StringType(), True),
    StructField("VEH_MOD_ID", StringType(), True),
    StructField("VEH_BODY_STYL_ID", StringType(), True),
    StructField("EMER_RESPNDR_FL", StringType(), True),
    StructField("OWNR_ZIP", StringType(), True),
    StructField("FIN_RESP_PROOF_ID", StringType(), True),
    StructField("FIN_RESP_TYPE_ID", StringType(), True),
    StructField("VEH_DMAG_AREA_1_ID", StringType(), True),
    StructField("VEH_DMAG_SCL_1_ID", StringType(), True),
    StructField("FORCE_DIR_1_ID", StringType(), True),
    StructField("VEH_DMAG_AREA_2_ID", StringType(), True),
    StructField("VEH_DMAG_SCL_2_ID", StringType(), True),
    StructField("FORCE_DIR_2_ID", StringType(), True),
    StructField("VEH_INVENTORIED_FL", StringType(), True),
    StructField("VEH_TRANSP_NAME", StringType(), True),
    StructField("VEH_TRANSP_DEST", StringType(), True),
    StructField("CONTRIB_FACTR_1_ID", StringType(), True),
    StructField("CONTRIB_FACTR_2_ID", StringType(), True),
    StructField("CONTRIB_FACTR_P1_ID", StringType(), True),
    StructField("VEH_TRVL_DIR_ID", StringType(), True),
    StructField("FIRST_HARM_EVT_INV_ID", StringType(), True),
    StructField("INCAP_INJRY_CNT", IntegerType(), True),
    StructField("NONINCAP_INJRY_CNT", IntegerType(), True),
    StructField("POSS_INJRY_CNT", IntegerType(), True),
    StructField("NON_INJRY_CNT", IntegerType(), True),
    StructField("UNKN_INJRY_CNT", IntegerType(), True),
    StructField("TOT_INJRY_CNT", IntegerType(), True),
    StructField("DEATH_CNT", IntegerType(), True)
])


# COMMAND ----------

#reading csv files 
df_charge_use = spark.read.load(path="dbfs:/FileStore/bcg_case/Charges_use.csv",format="csv",schema=charges_schema,sep=",",header=True)
df_damages_use = spark.read.load(path="dbfs:/FileStore/bcg_case/Damages_use.csv",format="csv",schema=damages_schema,sep=",",header=True)
df_endorse_use = spark.read.load(path="dbfs:/FileStore/bcg_case/Endorse_use.csv",format="csv",schema=endorsements_schema,sep=",",header=True)
df_primary_person_use = spark.read.load(path="dbfs:/FileStore/bcg_case/Primary_Person_use.csv",format="csv",schema=primary_person_schema,sep=",",header=True)
df_restrict_use = spark.read.load(path="dbfs:/FileStore/bcg_case/Restrict_use.csv",format="csv",schema=restrict_schema,sep=",",header=True)
df_units_use = spark.read.load(path="dbfs:/FileStore/bcg_case/Units_use.csv",format="csv",schema=units_schema,sep=",",header=True)

# COMMAND ----------

# MAGIC %md
# MAGIC 1.Analytics Find the number of crashes (accidents) in which number of males killed are greater than 2?

# COMMAND ----------

# Filter to include only males who were killed
males_killed_df = df_primary_person_use.filter((col("PRSN_GNDR_ID") == "MALE") & (col("DEATH_CNT") > 0))

# Group by crash ID and count the number of male deaths
crashes_with_male_deaths = males_killed_df.groupBy("CRASH_ID").sum("DEATH_CNT").withColumnRenamed("sum(DEATH_CNT)", "MALE_DEATH_CNT")

# Filter to include only crashes where the number of male deaths is greater than 2
crashes_gt_2_male_deaths = crashes_with_male_deaths.filter(col("MALE_DEATH_CNT") > 2)

# Count the number of such crashes
num_crashes = crashes_gt_2_male_deaths.count()

# Display the result
print(f"Number of crashes in which number of males killed are greater than 2: {num_crashes}")

# COMMAND ----------

# MAGIC %md
# MAGIC 2.Analysis How many two wheelers are booked for crashes?

# COMMAND ----------

# Filter to include only two wheelers
two_wheelers_df = df_units_use.filter(col("VEH_BODY_STYL_ID").like("%MOTORCYCLE%"))

# Count the number of such crashes
num_two_wheelers = two_wheelers_df.count()

# Display the result
print(f"Number of two wheelers booked for crashes: {num_two_wheelers}")

# COMMAND ----------

#display(df_units_use())

# COMMAND ----------

#df_units_use.select(col("VEH_BODY_STYL_ID")).distinct().show(truncate=False)

# COMMAND ----------


#df_units_use.select(col("UNIT_DESC_ID")).distinct().show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Analysis 3: Determine the Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy.

# COMMAND ----------

# Filter data to include only rows where airbags did not deploy and driver died
driver_died_no_airbag_df = df_primary_person_use.filter((col("PRSN_INJRY_SEV_ID") == "KILLED") & (col("PRSN_AIRBAG_ID") == "NOT DEPLOYED")&(col('PRSN_TYPE_ID')=='DRIVER'))

# Join with units_df to get vehicle makes
vehicles_in_crashes_df = driver_died_no_airbag_df.join(df_units_use, ["CRASH_ID"])

# Group by VEH_MAKE_ID and count the number of occurrences
top_vehicle_makes_df = vehicles_in_crashes_df.groupBy("VEH_MAKE_ID").count().orderBy(col("count").desc()).limit(5)

# COMMAND ----------

top_vehicle_makes_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Analysis 4: Determine number of Vehicles with driver having valid licences involved in hit and run?

# COMMAND ----------

# Filter data to include only valid licenses
valid_license_list=["NA","UNKNOWN","UNLICENSED"]
valid_licenses_df = df_primary_person_use.filter(~col("DRVR_LIC_CLS_ID").isin(valid_license_list)&((col('PRSN_TYPE_ID')=='DRIVER')))

# Join with units_df to get hit and run information
hit_and_run_df = valid_licenses_df.join(df_units_use , ["CRASH_ID"]).filter(col("VEH_HNR_FL") == "Y")

# Count the number of such vehicles
num_vehicles_hit_and_run = hit_and_run_df.count()

# Display the result
print(f"Number of vehicles with driver having valid licenses involved in hit and run: {num_vehicles_hit_and_run}")

# COMMAND ----------

#df_primary_person_use.select(col("DRVR_LIC_TYPE_ID")).distinct().show(truncate=False) 

# COMMAND ----------

# MAGIC %md
# MAGIC 5.Analysis Which state has highest number of accidents in which females are not involved? 

# COMMAND ----------

# Filter data to include only crashes without female involvement
no_females_df = df_primary_person_use.filter(col("PRSN_GNDR_ID") != "FEMALE")

# Group by state and count the number of crashes
state_accidents_df = no_females_df.groupBy("DRVR_LIC_STATE_ID").count().orderBy(col("count").desc()).limit(1)

# Display the result
state_accidents_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Analysis 6: Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death

# COMMAND ----------

# Calculate total injuries including death for each vehicle make
total_injuries_df = df_units_use.withColumn("TOTAL_INJURIES", col("INCAP_INJRY_CNT") + col("NONINCAP_INJRY_CNT") + col("POSS_INJRY_CNT") + col("DEATH_CNT"))

# Group by VEH_MAKE_ID and sum the total injuries
top_injuries_df = total_injuries_df.groupBy("VEH_MAKE_ID").sum("TOTAL_INJURIES").orderBy(col("sum(TOTAL_INJURIES)").desc()).limit(5)

# Select 3rd to 5th vehicle makes
third_to_fifth_vehicle_makes_df = top_injuries_df.select("VEH_MAKE_ID",row_number().over(Window.partitionBy(lit("")).orderBy(lit(""))).alias("rn")).filter((col('rn')>=3)&(col('rn')<=5))
# Display the result
print(f"Top 3rd to 5th VEH_MAKE_IDs that contribute to the largest number of injuries including death: {third_to_fifth_vehicle_makes_df.collect()[0:3]}")

# COMMAND ----------

# MAGIC %md
# MAGIC 7.Analysis For all the body styles involved in crashes, mention the top ethnic user group of each unique body style  

# COMMAND ----------

# Join primary_person_df with units_df to get body style information
body_style_ethnicity_df = df_primary_person_use.join(df_units_use, ["CRASH_ID"])  #, "UNIT_NBR"

# Group by body style and ethnicity and count the number of occurrences
body_style_ethnicity_count_df = body_style_ethnicity_df.groupBy("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID").count()

# Get the top ethnic user group for each unique body style
window_spec = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(col("count").desc())
top_ethnic_user_group_df = body_style_ethnicity_count_df.withColumn("rank", rank().over(window_spec)).filter(col("rank")==1).drop("rank")

# Display the result
top_ethnic_user_group_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 8.Analysis Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)

# COMMAND ----------

# Filter data to include only crashes with alcohol as a contributing factor
alcohol_contributing_df = df_primary_person_use.filter((col("PRSN_ALC_RSLT_ID") == "Positive"))

# Group by driver zip code and count the number of crashes
top_zip_codes_df = alcohol_contributing_df.groupBy("DRVR_ZIP").count().orderBy(col("count").desc()).limit(5)

# Display the result
top_zip_codes_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 9.Analysis Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance

# COMMAND ----------

#extract number from VEH_DMAG_SCL_1_ID and VEH_DMAG_SCL_2_ID
def extract_number(d_string):
  import re
  pattern = r"\d+"  # Matches one or more digits
  match = re.search(pattern, d_string)
  return int(match.group()) if match else None
#registring under udf 
extract_numbers_udf = udf(extract_number,(IntegerType()))

# COMMAND ----------

# Filter data to include only crashes with no damaged property and damage level above 4 and car avails insurance
no_damaged_property_df = df_damages_use.filter(col("DAMAGED_PROPERTY").isNull())
damage_level_above_4_df = df_units_use.filter((extract_numbers_udf("VEH_DMAG_SCL_1_ID") > 4) & (extract_numbers_udf("VEH_DMAG_SCL_2_ID") > 4))
insurance_df = df_units_use.filter((col("VEH_BODY_STYL_ID").like("%CAR%"))).filter(col("FIN_RESP_TYPE_ID")!= "NA")
#col("VEH_BODY_STYL_ID")
# Join the DataFrames and count distinct crash IDs
distinct_crash_ids_count = no_damaged_property_df.join(damage_level_above_4_df, "CRASH_ID").join(insurance_df, "CRASH_ID").select("CRASH_ID").distinct().count()

# Display the result
print(f"Count of Distinct Crash IDs with no damaged property, damage level above 4, and car avails insurance: {distinct_crash_ids_count}")

# COMMAND ----------

#insurance_df = df_units_use.filter((col("VEH_BODY_STYL_ID").like("%CAR%"))).filter(col("FIN_RESP_TYPE_ID") != "NA")
#insurance_df.select('*').display()

# COMMAND ----------

'''#unit test to verify
damage_level_above_4_df = df_units_use.filter((extract_numbers_udf("VEH_DMAG_SCL_1_ID") > 4) & (extract_numbers_udf("VEH_DMAG_SCL_2_ID") > 4)).filter((extract_numbers_udf("VEH_DMAG_SCL_1_ID")<4)|(extract_numbers_udf("VEH_DMAG_SCL_2_ID")<4)).display()'''

# COMMAND ----------

# MAGIC %md
# MAGIC 10.Analysis Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)

# COMMAND ----------

# Filter data to include only speeding related offences
speeding_offences_df = df_charge_use.filter(col("CHARGE").like("%SPEED%"))

# find the df_primary_person_use to get licensed drivers
licensed_drivers_df = df_primary_person_use.filter(((col('PRSN_TYPE_ID')=='DRIVER'))&(col("DRVR_LIC_TYPE_ID")=="DRIVER LICENSE"))

car_only_df = df_units_use.filter((col("VEH_BODY_STYL_ID").like("%CAR%")))


# Get top 10 used vehicle colours
top_vehicle_colours_df = df_units_use.groupBy("VEH_COLOR_ID").count().orderBy(col("count").desc()).limit(10)
top_vehicle_colours_list = [row["VEH_COLOR_ID"] for row in top_vehicle_colours_df.collect()]

#joining speeding_offences_df with df_primary_person_use and Get top 25 states with the highest number of offences

top_offence_states_df=licensed_drivers_df.join(car_only_df,["CRASH_ID"]).groupBy(col("DRVR_LIC_STATE_ID")).agg(count("CRASH_ID").alias('cnt')).orderBy(col('cnt').desc()).limit(25)
top_offence_states_list=[row["DRVR_LIC_STATE_ID"] for row in top_offence_states_df.collect()]

filtered_licensed_drivers_df=licensed_drivers_df.filter(col("DRVR_LIC_STATE_ID").isin(top_offence_states_list))
# Filter units_df to include only top 10 vehicle colours and top 25 offence states
filtered_units_df = df_units_use.filter(col("VEH_COLOR_ID").isin(top_vehicle_colours_list))

#filtered_units_df.display()
# Join the DataFrames
result_df = speeding_offences_df.join(filtered_licensed_drivers_df, ["CRASH_ID"]).join(filtered_units_df,["CRASH_ID"])

# Group by vehicle make and count the number of occurrences
top_vehicle_makes_speeding_df = result_df.groupBy("VEH_MAKE_ID").count().orderBy(col("count").desc()).limit(5)

# Display the result
top_vehicle_makes_speeding_df.show()

# COMMAND ----------

top_vehicle_colours_list

# COMMAND ----------

top_offence_states_list

# COMMAND ----------

