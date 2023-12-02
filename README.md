# You-Tube-Data-Harvesting

YouTube Data Harvesting and Warehousing project can be used to collect large amount of data from YouTube. The data can be stored in MongoDB and SQL and to visualize the data using sql query.

**Technology Used**
 * Python
 * MongoDB
 * MySQL
 * Streamlit Library

**Software Required**
* VSCode
* MongoDB Compass
* MySQL Workbench
* Updated web browser

**Libraries Required to install**

pip install google-api-python-client, pymongo, mysql, mysql-connector-python, pandas, streamlit.


**Approach of the project**

1.Connect to the YouTube API: Used the YouTube API to retrieve channel and video data. Used the Google API client library for Python to make requests to the API.

2.Store data in a MongoDB data lake: Once retrieved the data from the YouTube API, and that can store it in a MongoDB data lake. MongoDB is a great choice for a data lake because it can handle unstructured and semi-structured data easily.

3.Migrate data to a SQL data warehouse: After collected data for multiple channels, that can be migrated to a SQL data warehouse. Used a MySQL database.

4.Query the SQL data warehouse: Used SQL queries to join the tables in the SQL data warehouse and retrieve data for specific channels based on user input.

5.Display a Streamlit app: Streamlit is a great choice for building data visualization and analysis tools quickly and easily. Used Streamlit to create a simple UI where users can enter a YouTube channel ID, view the channel details, and select channels to migrate to the data warehouse.

Extracting Data from YouTube Console using our own API Key.

**Process Flow**

* In Data Extraction Input the Channel Id in the input text field.
* Next click on Extract data button will extract data and store in MongoDB.
* In DataTransformation of the application,migrate data we can choose the channel Nmae Which we can move to MySQL.
* In Queries you can select any of a query from the dropdown to see the detailed reports of collected data right below.

**License **
MIT License

**Summary **

This YouTube API Data project aims to provide a powerful tool for retrieving, analyzing, and visualizing YouTube data, enabling users to gain valuable information into channel performance and feedbacks.
