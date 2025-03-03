import pandas as pd
import plotly.express as px
import matplotlib.colors as mcolors
import numpy as np

import plotly.io as pio

pio.renderers.default = "browser"

# Lecture du fichier CSV avec Pandas
df = pd.read_csv("cleaned.csv")  # Assurez-vous que le chemin du fichier est correct

# Fonction pour générer une palette de couleurs en dégradé
def generate_gradient(start_color, end_color, num_colors):
    colors = [
        mcolors.rgb2hex(c)
        for c in mcolors.LinearSegmentedColormap.from_list("", [start_color, end_color])(np.linspace(0, 1, num_colors))
    ]
    return colors

# -------------------------------------------------------------------
# Répartition par tranche d'âge
age_counts = df.groupby("Age_band_of_driver").size().reset_index(name="count")
age_counts = age_counts.sort_values("count", ascending=False)
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

# -------------------------------------------------------------------
# Répartition par sexe
sex_counts = df.groupby("Sex_of_driver").size().reset_index(name="count")
sex_counts = sex_counts.sort_values("count", ascending=False)
num_sexes = sex_counts.shape[0]
gradient_colors = generate_gradient("#FF0000", "#FFFF00", num_sexes)
fig_sex_distribution = px.pie(
    sex_counts,
    names="Sex_of_driver",
    values="count",
    title="Répartition des accidents selon le sexe des conducteurs",
    color_discrete_sequence=gradient_colors,
)
fig_sex_distribution.show()

# -------------------------------------------------------------------
# Causes d'accident
causes_counts = df.groupby("Cause_of_accident").size().reset_index(name="count")
causes_counts = causes_counts.sort_values("count", ascending=False)
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

# -------------------------------------------------------------------
# Types de collision
collision_counts = df.groupby("Type_of_collision").size().reset_index(name="count")
collision_counts = collision_counts.sort_values("count", ascending=False)
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

# -------------------------------------------------------------------
# Conditions de lumière
light_conditions_counts = df.groupby("Light_conditions").size().reset_index(name="count")
light_conditions_counts = light_conditions_counts.sort_values("count", ascending=False)
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

# -------------------------------------------------------------------
# Niveau d'éducation
education_counts = df.groupby("Educational_level").size().reset_index(name="count")
education_counts = education_counts.sort_values("count", ascending=False)
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

# -------------------------------------------------------------------
# Conditions météorologiques
weather_counts = df.groupby("Weather_conditions").size().reset_index(name="count")
weather_counts = weather_counts.sort_values("count", ascending=False)
num_levels = weather_counts.shape[0]
gradient_colors = generate_gradient("#FF0000", "#FFFF00", num_levels)
fig_weather_conditions = px.bar(
    weather_counts,
    x="Weather_conditions",
    y="count",
    title="Conditions météorologiques lors des accidents",
    color="Weather_conditions",
    color_discrete_sequence=gradient_colors,
)
fig_weather_conditions.show()

# -------------------------------------------------------------------
# Type de surface de route
road_surface_counts = df.groupby("Road_surface_type").size().reset_index(name="count")
road_surface_counts = road_surface_counts.sort_values("count", ascending=False)
num_levels = road_surface_counts.shape[0]
gradient_colors = generate_gradient("#FF0000", "#FFFF00", num_levels)
fig_road_surface = px.bar(
    road_surface_counts,
    x="Road_surface_type",
    y="count",
    title="Types de surface de route lors des accidents",
    color="Road_surface_type",
    color_discrete_sequence=gradient_colors,
)
fig_road_surface.show()

# -------------------------------------------------------------------
# Sévérité des accidents
severity_counts = df.groupby("Accident_severity").size().reset_index(name="count")
severity_counts = severity_counts.sort_values("count", ascending=False)
num_levels = severity_counts.shape[0]
gradient_colors = generate_gradient("#FF0000", "#FFFF00", num_levels)
fig_accident_severity = px.pie(
    severity_counts,
    names="Accident_severity",
    values="count",
    title="Sévérité des accidents",
    color="Accident_severity",
    color_discrete_sequence=gradient_colors,
)
fig_accident_severity.show()

# -------------------------------------------------------------------
# Pourcentage des types d'accidents par sexe
total_cases_by_sex = df.groupby("Sex_of_driver").size().reset_index(name="Total_Count")
cases_by_sex_cause = df.groupby(["Sex_of_driver", "Cause_of_accident"]).size().reset_index(name="Count")
cases_by_sex_cause = cases_by_sex_cause.merge(total_cases_by_sex, on="Sex_of_driver")
cases_by_sex_cause["Percentage"] = round((cases_by_sex_cause["Count"] / cases_by_sex_cause["Total_Count"]) * 100, 2)
cases_by_sex_cause = cases_by_sex_cause.sort_values(["Cause_of_accident", "Sex_of_driver"])

color_map_sex = {"Male": "#0080FF", "Female": "#FD6C9E", "Unknown": "#B0B0B0"}
fig_cases_by_sex_cause = px.bar(
    cases_by_sex_cause,
    x="Cause_of_accident",
    y="Percentage",
    color="Sex_of_driver",
    barmode="group",
    title="Pourcentage des types d'accidents par sexe",
    labels={"Cause_of_accident": "Causes", "Percentage": "Percentage of Cases"},
    color_discrete_map=color_map_sex,
)
fig_cases_by_sex_cause.show()

# -------------------------------------------------------------------
# Pourcentage des types d'accidents par niveau d'éducation
total_cases_by_education = df.groupby("Educational_level").size().reset_index(name="Total_Count")
cases_by_education_cause = df.groupby(["Educational_level", "Cause_of_accident"]).size().reset_index(name="Count")
cases_by_education_cause = cases_by_education_cause.merge(total_cases_by_education, on="Educational_level")
cases_by_education_cause["Percentage"] = round((cases_by_education_cause["Count"] / cases_by_education_cause["Total_Count"]) * 100, 2)

color_map_education = {
    "High school": "#FD6C9E",
    "Junior high school": "#FF8000",
    "Elementary school": "#0080FF",
    "Above high school": "#643B9F",
    "Illiterate": "#FFD700",
    "Writing and reading": "#FF0000",
    "Unknown": "#B0B0B0",
}

fig_cases_by_education_cause = px.bar(
    cases_by_education_cause,
    x="Cause_of_accident",
    y="Percentage",
    color="Educational_level",
    barmode="group",
    title="Pourcentage des types d'accidents par niveau d'éducation",
    labels={"Cause_of_accident": "Causes", "Percentage": "Percentage of Cases"},
    color_discrete_map=color_map_education,
)
fig_cases_by_education_cause.show()

# -------------------------------------------------------------------
# Répartition selon l'expérience de conduite
# Normalisation de la colonne "Driving_experience" pour traiter les valeurs "unknown" ou "Unknown"
df["Driving_experience"] = df["Driving_experience"].apply(lambda x: "Unknown" if str(x).lower() == "unknown" else x)
experience_counts = df.groupby("Driving_experience").size().reset_index(name="count")
experience_counts = experience_counts.sort_values("count", ascending=False)
num_levels = experience_counts.shape[0]
gradient_colors = generate_gradient("#FF0000", "#FFFF00", num_levels)
fig_experience_distribution = px.bar(
    experience_counts,
    x="Driving_experience",
    y="count",
    title="Répartition des accidents selon l'expérience de conduite",
    color="Driving_experience",
    color_discrete_sequence=gradient_colors,
)
fig_experience_distribution.show()

# -------------------------------------------------------------------
# Pourcentage des types d'accidents par expérience de conduite
total_cases_by_experience = df.groupby("Driving_experience").size().reset_index(name="Total_Count")
cases_by_experience_cause = df.groupby(["Driving_experience", "Cause_of_accident"]).size().reset_index(name="Count")
cases_by_experience_cause = cases_by_experience_cause.merge(total_cases_by_experience, on="Driving_experience")
cases_by_experience_cause["Percentage"] = round((cases_by_experience_cause["Count"] / cases_by_experience_cause["Total_Count"]) * 100, 2)
cases_by_experience_cause = cases_by_experience_cause.sort_values(["Cause_of_accident", "Driving_experience"])

# Remarque : La carte de couleur ci-dessous est issue de l'exemple Spark, à ajuster selon les valeurs réelles de Driving_experience
color_map_experience = {"Male": "#0080FF", "Female": "#FD6C9E", "Unknown": "#B0B0B0"}
fig_cases_by_experience_cause = px.bar(
    cases_by_experience_cause,
    x="Cause_of_accident",
    y="Percentage",
    color="Driving_experience",
    barmode="group",
    title="Pourcentage des types d'accidents par expérience de conduite",
    labels={"Cause_of_accident": "Causes", "Percentage": "Percentage of Cases"},
    color_discrete_map=color_map_experience,
)
fig_cases_by_experience_cause.show()
