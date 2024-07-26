from pyspark.sql import SparkSession
import plotly.express as px
import matplotlib.colors as mcolors
import numpy as np
from pyspark.sql.functions import col, count, round, when

# Initialize SparkSession
spark = SparkSession.builder.appName("SparkApp").getOrCreate()

# Read data from CSV file
df = spark.read.csv("hdfs://localhost:9000/cleaned.csv", header=True)

df.printSchema()


# Function to generate gradient colors
def generate_gradient(start_color, end_color, num_colors):
    colors = [
        mcolors.rgb2hex(c)
        for c in mcolors.LinearSegmentedColormap.from_list(
            "", [start_color, end_color]
        )(np.linspace(0, 1, num_colors))
    ]
    return colors


# Age distribution
age_counts = (
    df.groupBy("Age_band_of_driver")
    .agg(count("*").alias("count"))
    .orderBy(col("count").desc())
)

age_counts_pandas = age_counts.toPandas()  # To visualize the data
num_levels = age_counts_pandas.shape[0]
gradient_colors = generate_gradient("#FF0000", "#FFFF00", num_levels)
fig_age_distribution = px.bar(
    age_counts_pandas,
    x="Age_band_of_driver",
    y="count",
    title="Répartition des accidents selon l'âge des conducteurs",
    color="Age_band_of_driver",
    color_discrete_sequence=gradient_colors,
)
fig_age_distribution.show()

# Sex distribution
sex_counts = (
    df.groupBy("Sex_of_driver")
    .agg(count("*").alias("count"))
    .orderBy(col("count").desc())
)

sex_counts_pandas = sex_counts.toPandas()  # To visualize the data
num_sexes = sex_counts_pandas.shape[0]
gradient_colors = generate_gradient("#FF0000", "#FFFF00", num_sexes)
fig_sex_distribution = px.pie(
    sex_counts_pandas,
    names="Sex_of_driver",
    values="count",
    title="Répartition des accidents selon le sexe des conducteurs",
    color_discrete_sequence=gradient_colors,
)
fig_sex_distribution.show()

# Causes of accident
causes_counts = (
    df.groupBy("Cause_of_accident")
    .agg(count("*").alias("count"))
    .orderBy(col("count").desc())
)

causes_counts_pandas = causes_counts.toPandas()  # To visualize the data
num_levels = causes_counts_pandas.shape[0]
gradient_colors = generate_gradient("#FF0000", "#FFFF00", num_levels)
fig_accident_causes = px.bar(
    causes_counts_pandas,
    x="Cause_of_accident",
    y="count",
    title="Causes d'accidents les plus courantes",
    color="Cause_of_accident",
    color_discrete_sequence=gradient_colors,
)
fig_accident_causes.show()

# Types of collision
collision_counts = (
    df.groupBy("Type_of_collision")
    .agg(count("*").alias("count"))
    .orderBy(col("count").desc())
)

collision_counts_pandas = collision_counts.toPandas()  # To visualize the data
num_levels = collision_counts_pandas.shape[0]
gradient_colors = generate_gradient("#FF0000", "#FFFF00", num_levels)
fig_collision_type = px.bar(
    collision_counts_pandas,
    x="Type_of_collision",
    y="count",
    title="Types de collisions les plus fréquents",
    color="Type_of_collision",
    color_discrete_sequence=gradient_colors,
)
fig_collision_type.show()

# Light conditions
light_conditions_counts = (
    df.groupBy("Light_conditions")
    .agg(count("*").alias("count"))
    .orderBy(col("count").desc())
)

light_conditions_counts_pandas = (
    light_conditions_counts.toPandas()
)  # To visualize the data
num_levels = light_conditions_counts_pandas.shape[0]
gradient_colors = generate_gradient("#FF0000", "#FFFF00", num_levels)
fig_light_conditions = px.bar(
    light_conditions_counts_pandas,
    x="Light_conditions",
    y="count",
    title="Conditions de lumière lors des accidents",
    color="Light_conditions",
    color_discrete_sequence=gradient_colors,
)
fig_light_conditions.show()

# Educational level
education_counts = (
    df.groupBy("Educational_level")
    .agg(count("*").alias("count"))
    .orderBy(col("count").desc())
)

education_counts_pandas = education_counts.toPandas()  # To visualize the data
num_levels = education_counts_pandas.shape[0]
gradient_colors = generate_gradient("#FF0000", "#FFFF00", num_levels)
fig_education_level = px.bar(
    education_counts_pandas,
    x="Educational_level",
    y="count",
    title="Répartition des accidents selon le niveau d'éducation des conducteurs",
    color="Educational_level",
    color_discrete_sequence=gradient_colors,
)
fig_education_level.show()

# Weather conditions
weather_counts = (
    df.groupBy("Weather_conditions")
    .agg(count("*").alias("count"))
    .orderBy(col("count").desc())
)

weather_counts_pandas = weather_counts.toPandas()  # To visualize the data
num_levels = weather_counts_pandas.shape[0]
gradient_colors = generate_gradient("#FF0000", "#FFFF00", num_levels)
fig_weather_conditions = px.bar(
    weather_counts_pandas,
    x="Weather_conditions",
    y="count",
    title="Conditions météorologiques lors des accidents",
    color="Weather_conditions",
    color_discrete_sequence=gradient_colors,
)
fig_weather_conditions.show()

# Road surface type
road_surface_counts = (
    df.groupBy("Road_surface_type")
    .agg(count("*").alias("count"))
    .orderBy(col("count").desc())
)

road_surface_counts_pandas = road_surface_counts.toPandas()  # To visualize the data
num_levels = road_surface_counts_pandas.shape[0]
gradient_colors = generate_gradient("#FF0000", "#FFFF00", num_levels)
fig_road_surface = px.bar(
    road_surface_counts_pandas,
    x="Road_surface_type",
    y="count",
    title="Types de surface de route lors des accidents",
    color="Road_surface_type",
    color_discrete_sequence=gradient_colors,
)
fig_road_surface.show()

# Accident severity
severity_counts = (
    df.groupBy("Accident_severity")
    .agg(count("*").alias("count"))
    .orderBy(col("count").desc())
)

severity_counts_pandas = severity_counts.toPandas()  # To visualize the data
num_levels = severity_counts_pandas.shape[0]
gradient_colors = generate_gradient("#FF0000", "#FFFF00", num_levels)
fig_accident_severity = px.pie(
    severity_counts_pandas,
    names="Accident_severity",
    values="count",
    title="Sévérité des accidents",
    color="Accident_severity",
    color_discrete_sequence=gradient_colors,
)
fig_accident_severity.show()

# Accidents causes by sex
total_cases_by_sex = df.groupBy("Sex_of_driver").agg(count("*").alias("Total_Count"))

cases_by_sex_cause = (
    df.groupBy("Sex_of_driver", "Cause_of_accident")
    .agg(count("*").alias("Count"))
    .join(total_cases_by_sex, "Sex_of_driver")
    .withColumn("Percentage", round((col("Count") / col("Total_Count")) * 100, 2))
    .orderBy("Cause_of_accident", "Sex_of_driver")
)

cases_by_sex_cause_pandas = cases_by_sex_cause.select(
    "Sex_of_driver", "Cause_of_accident", "Percentage"
).toPandas()  # To visualize the data

color_map = {"Male": "#0080FF", "Female": "#FD6C9E", "Unknown": "#B0B0B0"}

fig_cases_by_sex_cause = px.bar(
    cases_by_sex_cause_pandas,
    x="Cause_of_accident",
    y="Percentage",
    color="Sex_of_driver",
    barmode="group",
    title="Pourcentage des types d'accidents par sexe",
    labels={"Cause_of_accident": "Causes", "Percentage": "Percentage of Cases"},
    color_discrete_map=color_map,
)
fig_cases_by_sex_cause.show()


# Accidents causes by educational level
total_cases_by_education = df.groupBy("Educational_level").agg(
    count("*").alias("Total_Count")
)

cases_by_education_cause = (
    df.groupBy("Educational_level", "Cause_of_accident")
    .agg(count("*").alias("Count"))
    .join(total_cases_by_education, "Educational_level")
    .withColumn("Percentage", round((col("Count") / col("Total_Count")) * 100, 2))
)

cases_by_education_cause_pandas = cases_by_education_cause.select(
    "Educational_level", "Cause_of_accident", "Percentage"
).toPandas()  # To visualize the data

color_map = {
    "High school": "#FD6C9E",
    "Junior high school": "#FF8000",
    "Elementary school": "#0080FF",
    "Above high school": "#643B9F",
    "Illiterate": "#FFD700",
    "Writing and reading": "#FF0000",
    "Unknown": "#B0B0B0",
}

fig_cases_by_education_cause = px.bar(
    cases_by_education_cause_pandas,
    x="Cause_of_accident",
    y="Percentage",
    color="Educational_level",
    barmode="group",
    title="Pourcentage des types d'accidents par niveau d'éducation",
    labels={"Cause_of_accident": "Causes", "Percentage": "Percentage of Cases"},
    color_discrete_map=color_map,
)
fig_cases_by_education_cause.show()

# Driving experience
df_standardized = df.withColumn(
    "Driving_experience",
    when(col("Driving_experience").isin("unknown", "Unknown"), "Unknown").otherwise(
        col("Driving_experience")
    ),
)

experience_counts = (
    df_standardized.groupBy("Driving_experience")
    .agg(count("*").alias("count"))
    .orderBy(col("count").desc())
)

experience_counts_pandas = experience_counts.toPandas()  # To visualize the data
num_levels = experience_counts_pandas.shape[0]
gradient_colors = generate_gradient("#FF0000", "#FFFF00", num_levels)
fig_experience_distribution = px.bar(
    experience_counts_pandas,
    x="Driving_experience",
    y="count",
    title="Répartition des accidents selon l'expérience de conduite",
    color="Driving_experience",
    color_discrete_sequence=gradient_colors,
)
fig_experience_distribution.show()

# Accidents causes by driving experience
df_standardized = df.withColumn(
    "Driving_experience",
    when(col("Driving_experience").isin("unknown", "Unknown"), "Unknown").otherwise(
        col("Driving_experience")
    ),
)

total_cases_by_experience = df_standardized.groupBy("Driving_experience").agg(
    count("*").alias("Total_Count")
)

cases_by_experience_cause = (
    df.groupBy("Driving_experience", "Cause_of_accident")
    .agg(count("*").alias("Count"))
    .join(total_cases_by_experience, "Driving_experience")
    .withColumn("Percentage", round((col("Count") / col("Total_Count")) * 100, 2))
    .orderBy("Cause_of_accident", "Driving_experience")
)

cases_by_experience_cause_pandas = cases_by_experience_cause.select(
    "Driving_experience", "Cause_of_accident", "Percentage"
).toPandas()  # To visualize the data

color_map = {"Male": "#0080FF", "Female": "#FD6C9E", "Unknown": "#B0B0B0"}

fig_cases_by_experience_cause = px.bar(
    cases_by_experience_cause_pandas,
    x="Cause_of_accident",
    y="Percentage",
    color="Driving_experience",
    barmode="group",
    title="Pourcentage des types d'accidents par expérience de conduite",
    labels={"Cause_of_accident": "Causes", "Percentage": "Percentage of Cases"},
    color_discrete_map=color_map,
)
fig_cases_by_experience_cause.show()


# Stop SparkSession
spark.stop()
