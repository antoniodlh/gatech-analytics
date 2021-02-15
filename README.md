CSE 6242 - Study of Small-Medium Businesses & Crime during COVID-19
by Antonio De Los Heros, Muhammad Ibraheem, Muhammad Kaleem Khan, Chandra Narasimhan, Raghvendra Trivedi, John Walker

a) Description
A graduate-course project to analyze the impact of COVID-19 on small-medium businesses (SMB) and crime, as well as correlations
between those factors. The analysis was performed using zip level data from the following cities: Baltimore, Boston, Denver, and Phoenix.
Publicly available Covid, crime and small business datasets were used to perform the detailed analysis.
Key datasets and Level of data granularity are given below:  
Crime – Latitude/Longitude
Covid – County level
Small and medium business(SMB) data – Zip code level

Crime and covid datasets were mapped to zip code level data using zip code reference data.
SMB data from multiple datasets were merged to extract required revenue data. Data standardization and data cleansing techniques were applied.
All 3 data groups were joined at data, zip code level to create a consolidated Crime, Covid, SMB dataset.  
Regression, time series and K-means clustering analysis were performed on consolidated dataset.
Interactive Tableau dashboards were created to visualize output of analysis.  

b) Data Sources
1. The New York Times COVID-19 Data - https://github.com/nytimes/covid-19-data/blob/master/us-counties.csv
2. US Zip Codes Database - https://simplemaps.com/data/us-zips
3. Python-based Zip Code Database - https://pypi.org/project/uszipcode/
4. Baltimore Crime Data - https://data.baltimorecity.gov/Public-Safety/BPD-Part-1-Victim-Based-Crime-Data/wsfq-mvij
5. Boston Crime Data - https://data.boston.gov/dataset/crime-incident-reports-august-2015-to-date-source-new-system/resource/12cb3883-56f5-47de-afa5-3b1cf61b257b
6. Denver Crime Data - https://www.denvergov.org/opendata/dataset/city-and-county-of-denver-crime
7. Phoenix Crime Data - https://www.phoenixopendata.com/dataset/crime-data/resource/0ce3411a-2fc6-4302-a33f-167f68608a20
8. SMB Data - https://github.com/OpportunityInsights/EconomicTracker

c) Installation
Minimum Computer Requirements:
-8GB RAM
-256GB disk
-Dual-core Core i3 or better

Pre-requisites
To execute data preparation and visualization, Install the following software:
1) Python 3.7.x or higher
2) pyspark 3.0.1 or higher
3) Jupyter Notebook
4) R 4.0.3 higher
5) R studio
6) Tableau 2020.3 or higher

Download data files from data sources(section b) to local file directory. Download source code files from source directory to local file directory.
upload script files and data files to Jupyter notebook. Modify your scripts to match your input and output file names(optional).

d) Data Preparation

Execute the scripts the following order:
1. Data cleansing and standardizing Covid Zip level data  - Run 'Covid_dataprep.ipynb'
2. Data cleansing and standardizing Crime Zip level data  - Run all 4 city scripts 'Data Load - Phoenix.py', 'Data Load - Denver.py', 'Data Load - Boston.py', 'Data Load - Baltimore.py'
3. Merge Crime Zip data files - Run 'Combine_Clean.py'
4. Standardize and merge SMB data files - Run 'SMBProcess.ipynb'
5. Combine Covid, Crime and SMB data to create one integrated data set - Run 'Data_Integration.ipynb'

e) Data Analysis
Perform the data analysis by executing the following scripts:
1) Regression analysis of combined Covid, Crime, SMB data - Run 'Regression.R'
2) K-means clustering of combined Covid, Crime, SMB data - Run 'Kmeans.Rmd'


f) Execution

To perform interactive visualizations, download Dashboard_v2.twb and import it to your tableau desktop software.
Modify Dashboard_v2.twb data sources to match your dataset names.
Tableau dashboards consists of number of sheets to perform data visualizations. Key visualizations:
SMB Monthly Revenue change - Bar charts and Line charts
Covid cases and Crime - Bubble charts
Comparison of Covid cases and deaths in US Zip codes  - choropleth map  
Top Zip codes with covid cases, violent and non-violent crimes - Horizontal Bar Charts
Covid, Violenet/non-Violent Crime relationship - Node Graph  

g) Demo

Small-Medium Businesses and Crime during COVID-19 dashboard is publicly available and can be accessed using the following link:
https://public.tableau.com/profile/muhammad.ibraheem8383#!/vizhome/Dashboard_v2_16048870662570/VisualizationDashboardModified?publish=yes

Dashboard is highly interactive. Dashboard users can perform city level analysis by selecting a city from city drop down list.
Visualization will refresh to show city level analysis.
By pressing play button on the top right hand corner, you can view change in monthly crime/covid changes across zip codes for the selected city.
Mouseover tool tips are available on all graphs.
Dashboard provides additional links to explore node graphs (Node_Edge) and time series analysis(Time Series)
Both node graphs and time series graphs provide interactive features to explore the data.
