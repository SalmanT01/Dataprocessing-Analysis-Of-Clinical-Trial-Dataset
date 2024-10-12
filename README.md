# Project Overview

The aim of this academic project is to implement data processing and analysis methods on a clinical trial dataset. This effort seeks to extract in-depth insights and valuable information regarding clinical trial studies. The analysis will leverage powerful tools, particularly PySpark on the Databricks platform, and will incorporate Python and SQL for data manipulation and querying. 

## Datasets Utilized

The project utilizes two main CSV datasets: 

1. **Clinical Trial Dataset (2023):** This dataset contains comprehensive information regarding clinical trials conducted in the year 2023.
2. **Pharmaceutical Companies Dataset:** This dataset provides details about various pharmaceutical companies involved in clinical trials.

Additionally, datasets from the years 2020 and 2021 will be utilized to evaluate and refine data processing strategies but will not be the focus of the primary analysis.

# Databricks Setup

The project is conducted on the Databricks platform, which offers robust storage capabilities and a cluster-based computational system ideal for large-scale data processing. Within Databricks, separate workspaces will be established for each data processing approach—namely, Resilient Distributed Datasets (RDD), DataFrames, and SQL. Each workspace will include executable code and descriptive documentation detailing the steps involved in dataset processing. 

The importance of correctly preparing and structuring the dataset cannot be overstated, as improper data handling can complicate the analysis process. Thus, a series of preprocessing steps will be applied to ensure data integrity.

# Data Processing Steps

## 1. Data Import

The first step involves uploading the zipped datasets—**ClinicalTrial2023** and **Pharma**—to the Databricks File System (DBFS). These files will be placed in the directory designated as `("FileStore/tables/")`, allowing for easy access and organization within the Databricks environment.

## 2. Unzipping Files

Once the datasets are uploaded, the next step is to unzip the files. This is accomplished using Linux shell commands within Databricks. The zipped files are copied from DBFS to the local filesystem, and a new folder titled **Assessment_Task1_Folder** is created at `("file:/tmp/Assessment_Task1_Folder/")`. In this folder, both zipped files are extracted, generating the necessary `.csv` files for analysis.

## 3. Data Preparation and Cleaning in RDD

The cleaning process is commenced by inspecting the first two rows of the clinical trial dataset in its raw format. The dataset is found to contain unwanted characters, including tab separators (`"\t"`) between column values, and extraneous double quotes (`"`), along with a series of commas (`,`) interspersed throughout the rows.

### Cleaning Process
- **Data Manipulation:** To address these issues, RDD operations is employed. RDDs allow for various transformations and actions on dataset elements in PySpark. A common operation used is **Map transformation**, which enables manipulation of data values on both row and column levels.
  
  - **Removing Unwanted Characters:** 
    - The leading and trailing double quotes will be removed from each row using a Python lambda function.
    - The series of commas following each row will be eliminated using the `strip` command.
    
# Analysis of Clinical Trial 2023 Dataset

## Question 1: Find Number of Distinct Studies

### Assumptions
Each study is assigned a unique ID in the `ID` column. Counting the unique IDs will yield the total number of studies.

### PySpark Implementation Outline

#### RDD Approach
1. **Remove Header Row**: Use the `filter` operation to eliminate the head row from `RDD3`, resulting in `RDD3_NoHead`.
2. **Create Key-Value Pair RDD**: 
   - Map transformation creates key-value pairs where the key is the ID column and the value is the study.
3. **Distinct Action**: 
   - Use the `distinct()` action to remove duplicates.
4. **Count Action**: 
   - Apply the `count()` action to determine the number of unique IDs, resulting in approximately **483,421** distinct studies.

#### DataFrame Approach
1. **Select ID Values**: 
   - Extract ID values from `myRDD_DF1` using `select()` and store in `DF_ID_Column`.
2. **Count Unique IDs**: 
   - Apply the `count()` function to `DF_ID_Column` to get the total number of unique studies.

#### SQL Implementation Outline
1. **SQL Query**: 
   - Use `SELECT COUNT(DISTINCT ID)` on the `sqlclinicaltrial1` table to count the unique IDs.

### Discussion of Results
Comparing outputs from all three approaches reveals a discrepancy of 1 record, confirming the number of distinct studies to be approximately **483,421** with a negligible error due to potential data extraction inconsistencies.

---

## Question 2: List Types with Frequency

### PySpark Implementation Outline

#### RDD Approach
1. **Remove Header**: Filter out the header row from `Type_rdd`.
2. **Key-Value Pair RDD**:
   - Map transformation creates a pair where each type is the key and the value is set to `1`.
3. **Reduce by Key**: 
   - Use `reduceByKey()` to sum the values, yielding frequency counts.
4. **Sort by Frequency**: 
   - Use `sortBy()` to arrange the results in descending order.

#### DataFrame Approach
1. **Group Types**: 
   - Create a DataFrame `DF_Type_Column` and use `groupBy()` to group by types.
2. **Count Frequencies**: 
   - Apply the `count()` function to count occurrences of each type.
3. **Sort**: 
   - Use `orderBy()` to sort by the count column in descending order.

#### SQL Implementation Outline
1. **SQL Query**: 
   - Use `SELECT Type, COUNT(*)` grouped by type from `clinicalTrialTable`.

### Discussion of Results
All three approaches yield similar frequency outputs, confirming the maximum number of trial studies belong to the **Interventional** type, with a minimum of **889** trials having no specified type.

---

## Question 3: Top 5 Conditions Linked to Studies

### PySpark Implementation Outline

#### RDD Approach
1. **Split Conditions**: 
   - Use `map` to split conditions using the delimiter ('|').
2. **FlatMap Transformation**: 
   - Use `flatMap` to create separate rows for each condition.
3. **Key-Value Pair Creation**:
   - Convert to key-value pairs with each condition as key and `1` as value.
4. **Aggregate and Sort**:
   - Use `reduceByKey()` to sum values and `sortBy()` to arrange results by frequency.

#### DataFrame Approach
1. **Split and Explode**: 
   - Use `split()` followed by `explode()` to separate values into different rows.
2. **Group and Count**: 
   - Group by condition and apply `count()`.
3. **Sort**: 
   - Use `orderBy()` to sort the DataFrame by frequency.

#### SQL Implementation Outline
1. **SQL Query**: 
   - Use `SELECT Condition, COUNT(*)` with a `GROUP BY` statement to count conditions.

### Discussion of Results
All approaches confirm the top 5 conditions are **Healthy**, **Breast Cancer**, **Obesity**, **Stroke**, and **Hypertension**, with **Healthy** being the most common condition.

---

## Question 4: Top 10 Non-Pharma Sponsors

### PySpark Implementation Outline

#### RDD Approach
1. **Extract Pharma Names**: 
   - Load `pharma.csv` into `RDD_Pharma`, applying map transformations to clean data.
2. **Store Parent Companies**: 
   - Extract and store the `parent_company` column as a list in `Result2023`.
3. **Sponsor Frequency Count**:
   - Pair sponsors as key-value with value `1`, reduce and filter out pharma companies based on `Result2023`.

#### DataFrame Approach
1. **Extract Unique Pharma Names**: 
   - Directly select and distinct the `Parent_Company` column from the pharma DataFrame.
2. **Group and Count Sponsors**: 
   - Group the sponsors and count occurrences.
3. **Filter Non-Pharma Sponsors**: 
   - Apply `filter()` with conditional `isin()` against `Pharma_Name_List`.

#### SQL Implementation Outline
1. **Preprocessing Pharma Data**: 
   - Clean the `pharma.csv` and create a temporary SQL view for pharma names.
2. **SQL Query**: 
   - Use a nested query to select sponsors that do not exist in the pharma names list.

### Discussion of Results
All three methods confirm the top 10 sponsors, with the **National Cancer Institute (NCI)** being the most common. An exception is noted with **Novartis Pharmaceuticals**, which is included despite being a pharma company.

---

## Question 5: Completed Studies in 2023

### PySpark Implementation Outline

#### RDD Approach
1. **Remove Header**: Filter out the header row to acquire values.
2. **Extract Dates**: 
   - Filter dates for the year 2023, storing in `RDD_Dates3`.
3. **Month Extraction**: 
   - Use `map` to extract month and year information.
4. **Count by Month**: 
   - Transform into key-value pairs and reduce by month.

#### DataFrame Approach
1. **Filter Dates**: 
   - Use `filter()` to extract dates for 2023.
2. **Extract Month**: 
   - Create a new column for the month using the `month()` function.
3. **Group and Count**: 
   - Group by month and sum counts.

#### SQL Implementation Outline
1. **SQL Query**: 
   - Use `SELECT MONTH(Completed_Date), COUNT(*)` for year 2023, grouping by month.

### Discussion of Results
All methods show that December had the highest number of completed studies, while February had the least. A line plot of the output results can be generated to visualize the counts across months.

---


