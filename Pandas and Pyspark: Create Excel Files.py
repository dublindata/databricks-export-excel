# Databricks notebook source
# MAGIC %md
# MAGIC # Python and Pandas Excel File Creation
# MAGIC
# MAGIC This notebook can be used to show off how you can take a Spark DataFrame that was used to select data from your Lakehouse source(s), and then using the power of the Pandas on Spark API (https://docs.databricks.com/en/pandas/pandas-on-spark.html), easily convert the result set to a single Excel file for then export to a location like a Unity Catalog volume (https://docs.databricks.com/en/connect/unity-catalog/volumes.html). Once there, your files can be sent using automation like SMTP (via a relevant server), copy or (S)FTP options, or really whatever you want.
# MAGIC
# MAGIC ## How does it work?
# MAGIC
# MAGIC Pandas (https://pandas.pydata.org/) is a well-known and loved flexible and easy to use open source data analysis and manipulation tool that has a lot of commonalities with Apache Spark in that you can use it to read in data from a multitude of sources, analyse and transform it, and then write it back out. Previously, Spark and Pandas were "mutually exclusive" because while people like to use Pandas, it isn't a distributed framework. Spark, on the other hand, is designed to work with large distributed datasets at scale. To bridge the gap, there is a Pandas on Spark API specification that you can use to transform Spark DataFrames to and/or from Pandas Dataframes, and even use Pandas APIs on Spark to still leverage APIs that developers and analytists may already be comfortable with while not compromsing scale.
# MAGIC
# MAGIC This particular solition takes advantage of a method called ```toPandas()``` to convert a Spark DataFrame to a Pandas DataFrame, and then the ```to_excel()``` method to write out the data to an Excel file. It also includes a more programitic example on how use Pandas and some open source Excel writing libraries in Python to apply formatting to header columns and renaming sheets.
# MAGIC
# MAGIC Ready? Let's check it out!

# COMMAND ----------

# MAGIC %md
# MAGIC ### Importing Libraries
# MAGIC
# MAGIC While Databricks clusters already have the Pandas on Spark libraries as part of our clusters, we need to import them to work with them. This next command will load the Pandas on Spark library into our session. It's important to note that is ISN'T the regular Pandas library, as we'll see later.

# COMMAND ----------

import pyspark.pandas as ps

# COMMAND ----------

# MAGIC %md
# MAGIC ### Installing Excel Writing Libraries
# MAGIC
# MAGIC While the library we're going to use to write our Excel files has a method to write them, we need to add a couple other libraries to our cluster to support that; really, we only need one of these, but I'm installing both here. This method is using ```pip``` to do the install, but you can also use the Libaries feature of your Databricks cluster(s) to manage this too (https://docs.databricks.com/en/libraries/cluster-libraries.html)

# COMMAND ----------

# MAGIC %pip install openpyxl xlsxwriter

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting our data
# MAGIC
# MAGIC Now, we'll use Spark to select the data we want. There's lots of ways to do this, but if we already have our data loaded to tables in our Unity Catalog, we can use Spark SQL to write a SQL statement to select it. Of course, any other Spark read statment works here, from raw files, Spark structured streaming, etc. But this makes it easy to see what we want to select and use ANSI SQL to do it. The resulting data will stored in our DataFrame variable, ```df```. The second cell below is optional, and you can remove it if you don't want to preivew the results.

# COMMAND ----------

df = spark.sql("SELECT * FROM main.sampledatabase.sample_taxi_trips_identity LIMIT 10")

# COMMAND ----------

df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Convert the Spark DataFrame to a Pandas DataFame
# MAGIC
# MAGIC Next, we'll do an conversion of our Spark DataFrame to Pandas. Remember, the reason we have to do this is because Pandas has native support for writing Excel, but Spark doesn't (at least, not without a custom Spark connector). This is done in one simple line to covert, and our results are in a new variable, ```psdf```. Note: Since Pandas DataFrames are a single-node/non-distrubted structure, this will essentially "pull" all of your distrubted data to your Spark Driver (aka Main) node. Be mindful of large datasets and running your node out of memory! 
# MAGIC
# MAGIC The second cell below outputs our dataset, like we did above, and is optional, but you can see that by doing this the output looks different.

# COMMAND ----------

psdf = df.toPandas()

# COMMAND ----------

psdf

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing an Excel file
# MAGIC
# MAGIC The next command is all we need to do to write an Excel file. It just takes our data, and writes it out to a new file that we specify in the command. What's the MOST important thing here to realize is just "where" it writes this file. It's actually writing to *the local filesystem of the main driver node of your cluster!* Not a volume, not the Databricks File System (dbfs), or anyhting else. Once the file is written, it needs to be copied or moved somewhere. We also can't write this file to a location, like a cloud storage account or volume directly, because the paths will be "not available" or the protocols don't exist for us to use Unity Catalog external locations, either. Instead, we need to use command line or file system utilities to move the file.
# MAGIC
# MAGIC The cells below the next one are mean to show that process; We use magic ```%sh``` commands to use command Linux commands like ```cp``` or ```mv``` to copy the files to different locations. In this example, I am writing a Unity Catalog volume. The commands are meant to show a "before and after" file system listing of the file moving.

# COMMAND ----------

psdf.to_excel("excel_file.xlsx")

# COMMAND ----------

# MAGIC %sh 
# MAGIC ls

# COMMAND ----------

# MAGIC %sh 
# MAGIC mv excel_file.xlsx /Volumes/main/sampledatabase/sample_volume_2/excel_file.xlsx

# COMMAND ----------

# MAGIC %sh
# MAGIC ls

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting Fancy: Formatting Headers in our Excel File
# MAGIC
# MAGIC To take this a step further, we use a programitic approach to format our Excel file with things like bold text, renaming sheets, or even changing colors of cells. To do this, we need to add a few lines of code to our solution, and take more advantage of our libraries we installed before. But before we do, we need the "base" Pandas library loaded, so we'll do that first. Then, we'll create a new, empty Excel file using the ```ExcelWriter()``` method to create a new file. Then, we'll take that same DataFrame we used about ```psdf``` and write to our new file. This looks really similar to what we did before, but one thing you might notice is when we use the ```writer``` object, we're specifying an "engine" library. This will let us features of that library to format our file. Also important for this example is that we're using ```startrow``` to leave our first row (row zero) empty so we can add formatted headers later.

# COMMAND ----------

import pandas as pd

# COMMAND ----------

writer = pd.ExcelWriter("excel_with_header.xlsx", engine="xlsxwriter")

# COMMAND ----------

psdf.to_excel(writer, sheet_name="Drews Sheet", startrow=1, header=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Now that the data is written out, let's apply some formatting! First, we use our existing writer object to get our sheet we want to work with. Then, we create a new dictionary that contains our formatting options, like bold text, colors, etc. We'll use this to apply to our header row soon. For more info on the ```add_format``` method, see https://xlsxwriter.readthedocs.io/format.html

# COMMAND ----------

workbook=writer.book
worksheet = writer.sheets["Drews Sheet"]

header_format = workbook.add_format(
    {
        "bold": True,
        "text_wrap": True,
        "valign": "top",
        "fg_color": "#D7E4BC",
        "border": 1,
    }
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing the header row
# MAGIC
# MAGIC Since we didn't do it already (by starting at row 1), we need to add our headers back with our fomratting. We can do that enumerating the columns in our ```psdf``` DataFrame, and write them to row zero and increment the column each time, and use the fomatting options we used above for the value. Again, normally, the headers are written as part of our Excel export option, but we specifically didn't do that here to apply formatting to a specific row in our file. These are the kind of steps you need to take for this sort of control, so it's a little "give and take" if you will.
# MAGIC
# MAGIC Once we write our header row, we'll issuse a ```close()``` command on our file to tell Python we're done writing to it. All we have to do now is move it/copy it/send it, etc. But before we do that...

# COMMAND ----------

for col_num, value in enumerate(psdf.columns.values):
    worksheet.write(0, col_num + 1, value, header_format)

# COMMAND ----------

writer.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using ```shutil``` to copy the Excel file
# MAGIC
# MAGIC Previously, we just used magic shell commands to move our file; the problem with that method, is that while it works, we can't trap and respond to errors or exceptions in copying the files, for instance if the user doesn't have access to the target location. If we use a Python command instead, exceptions "bubble up" and can be acted on or reported (or caught with Workflows as failed runs). You may want to consider using methods like this if you plan to automate this process. 

# COMMAND ----------

import shutil

shutil.move("excel_with_header.xlsx", "/Volumes/main/sampledatabase/sample_volume_2/excel_with_header.xlsx")
