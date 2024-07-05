# Car Accidents Analysis with Hadoop

## Project Description
This project aims to analyze car accident data to extract useful statistics. The main goal is to identify trends and patterns in accident data to propose improved safety measures. This project utilizes Hadoop for processing large-scale data.

## Prerequisites
Before starting, ensure you have the following installed:
- Hadoop
- Java
- Gradle (for dependency management)

## Installation
1. Clone this repository:
   ```bash
   git clone https://github.com/your-username/your-repo.git
   ```
2. Navigate to the project directory:
   ```bash
   cd your-repo
   ```
3. Run the initialization script to set up the environment and download sample data:
   ```bash
   ./init.sh
   ```
4. Build the project and fetch dependencies:
   ```bash
   gradle build
   ```

## Execution
### Automatic Method
To run all queries, use the following command:
```bash
./run.sh runAll
```
You can also run specific queries by passing them as arguments:
```bash
./run.sh ContributingFactors LethalPerWeek WeekBorough
```

## Project Structure
- `src/main/java`: Contains Java source files for MapReduce tasks.
- `data`: Contains input data (CSV files).
- `output`: Contains analysis results.

