import pandas as pd

# Lecture du fichier CSV d'origine
df = pd.read_csv("RTA Dataset.csv", encoding="utf-8", sep=",")

# Affichage de la forme initiale
print("Forme initiale :", df.shape)

# Suppression des colonnes inutiles
df = df.drop(columns=['Time', 'Day_of_week', 'Type_of_vehicle', 'Owner_of_vehicle', 'Service_year_of_vehicle', 'Defect_of_vehicle', 'Area_accident_occured', 'Road_allignment',
                      'Road_surface_conditions', 'Number_of_vehicles_involved', 'Number_of_casualties', 'Casualty_class', 'Sex_of_casualty', 'Age_band_of_casualty', 
                      'Casualty_severity', 'Work_of_casuality', 'Fitness_of_casuality'])

# Suppression des doublons
df = df.drop_duplicates()

# Gestion des valeurs manquantes
df = df.dropna()

# Nettoyage des noms de colonnes (suppression des espaces superflus et mise en minuscule)
df.columns = df.columns.str.strip()

# Nettoyage des espaces superflus dans les colonnes de type texte
for col in df.select_dtypes(include=['object']).columns:
    df[col] = df[col].str.strip()

# Conversion des colonnes de type date (si le nom de colonne contient 'date')
for col in df.columns:
    if 'date' in col:
        try:
            df[col] = pd.to_datetime(df[col])
        except Exception as e:
            print(f"Erreur lors de la conversion de la colonne {col} en datetime :", e)

# Affichage de la forme après nettoyage
print("Forme après nettoyage :", df.shape)

# Sauvegarde du DataFrame nettoyé dans un nouveau fichier CSV
df.to_csv("cleaned.csv", index=False, encoding="utf-8")

print("Fichier cleaned.csv sauvegardé avec succès.")
