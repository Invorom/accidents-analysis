from pyspark.sql import SparkSession, col, count
import plotly.express as px
import matplotlib.colors as mcolors
import numpy as np

# Initialize SparkSession
spark = SparkSession.builder.appName("SparkApp").getOrCreate()

# Read data from CSV file
df = spark.read.csv("hdfs://localhost:9000/cleaned.csv", header=True)


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

# Stop SparkSession
spark.stop()
