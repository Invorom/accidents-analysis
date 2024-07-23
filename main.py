from pyspark.sql import SparkSession
import plotly.express as px
import matplotlib.colors as mcolors
import numpy as np

# Initialize SparkSession
spark = SparkSession.builder.appName("SparkApp").getOrCreate()

# Read data from CSV file
df = spark.read.csv("hdfs://localhost:9000/cleaned.csv", header=True)


# Print schema
df.printSchema()

data = df.toPandas()


def generate_gradient(start_color, end_color, num_colors):
    colors = [
        mcolors.rgb2hex(c)
        for c in mcolors.LinearSegmentedColormap.from_list(
            "", [start_color, end_color]
        )(np.linspace(0, 1, num_colors))
    ]
    return colors


age_counts = data["Age_band_of_driver"].value_counts().reset_index()
age_counts.columns = ["Age_band_of_driver", "count"]
num_levels = age_counts.shape[0]
gradient_colors = generate_gradient("#FF0000", "#FFFF00", num_levels)
fig_age_distribution = px.bar(
    age_counts,
    x="Age_band_of_driver",
    y="count",
    title="Répartition des accidents selon l'âge des conducteurs",
    color="Age_band_of_driver",
    color_discrete_sequence=gradient_colors,
)
fig_age_distribution.show()

sex_counts = data["Sex_of_driver"].value_counts().reset_index()
sex_counts.columns = ["Sex_of_driver", "count"]
num_sexes = sex_counts.shape[0]
gradient_colors = generate_gradient("#FF0000", "#FFFF00", num_sexes)
fig_sex_distribution = px.pie(
    sex_counts,
    names="Sex_of_driver",
    values="count",
    title="Répartition des accidents selon le sexe des conducteurs",
    color="Sex_of_driver",
    color_discrete_sequence=gradient_colors,
)
fig_sex_distribution.show()

causes_counts = data["Cause_of_accident"].value_counts().reset_index()
causes_counts.columns = ["Cause_of_accident", "count"]
num_levels = causes_counts.shape[0]
gradient_colors = generate_gradient("#FF0000", "#FFFF00", num_levels)
fig_accident_causes = px.bar(
    causes_counts,
    x="Cause_of_accident",
    y="count",
    title="Causes d'accidents les plus courantes",
    color="Cause_of_accident",
    color_discrete_sequence=gradient_colors,
)
fig_accident_causes.show()


collision_counts = data["Type_of_collision"].value_counts().reset_index()
collision_counts.columns = ["Type_of_collision", "count"]
num_levels = collision_counts.shape[0]
gradient_colors = generate_gradient("#FF0000", "#FFFF00", num_levels)
fig_collision_type = px.bar(
    collision_counts,
    x="Type_of_collision",
    y="count",
    title="Types de collisions les plus fréquents",
    color="Type_of_collision",
    color_discrete_sequence=gradient_colors,
)
fig_collision_type.show()


light_conditions_counts = data["Light_conditions"].value_counts().reset_index()
light_conditions_counts.columns = ["Light_conditions", "count"]
num_levels = light_conditions_counts.shape[0]
gradient_colors = generate_gradient("#FF0000", "#FFFF00", num_levels)
fig_light_conditions = px.bar(
    light_conditions_counts,
    x="Light_conditions",
    y="count",
    title="Conditions de lumière lors des accidents",
    color="Light_conditions",
    color_discrete_sequence=gradient_colors,
)
fig_light_conditions.show()

education_counts = data["Educational_level"].value_counts().reset_index()
education_counts.columns = ["Educational_level", "count"]
num_levels = education_counts.shape[0]
gradient_colors = generate_gradient("#FF0000", "#FFFF00", num_levels)
fig_education_level = px.bar(
    education_counts,
    x="Educational_level",
    y="count",
    title="Répartition des accidents selon le niveau d'éducation des conducteurs",
    color="Educational_level",
    color_discrete_sequence=gradient_colors,
)
fig_education_level.show()


# Stop SparkSession
spark.stop()
