# Tekken Subreddit ETL Pipeline

Hello! This project demonstrates the process of extracting data from the Tekken subreddit, transforming it, and visualizing the results. The data is collected using the PRAW library, stored in AWS S3, processed using AWS Glue, and visualized using Looker Studio.

![image](https://github.com/user-attachments/assets/feb8f4b4-491c-45f0-9b15-8efe13ed65c4)



## Project Workflow

- **Data Extraction**:<br>
   - Used PRAW (Python Reddit API Wrapper) to extract data from the Tekken subreddit.
     Extracted fields include Title, Score, Upvote_Ratio, Number_of_Comments, Url, Author, Flair, and Created_UTC.
  <br>
 The extracted data was converted as a CSV file.
  <br>
- **Data Storage**:<br>
  - Uploaded the CSV file to an S3 bucket on AWS for storage.<br>
- **Data Cataloging**:<br>
  - Created a Glue Crawler to automatically catalog the CSV file stored in S3.<br>
  - The Glue Crawler generated a table in the AWS Glue database based on the CSV data.<br>
- **Data Transformation (ETL)**:<br>
	 - Used AWS Glue's visual editor to perform the following ETL operations:<br>
	 - Remove Nulls: Identified and removed columns containing only null values.<br>
	 - Timestamp Transformation: Converted the UNIX timestamp (Created_UTC) to a human-readable date format.<br>
	 - SQL Query: Executed a SQL query to sort the data by the Year column.<br>
	 - The transformed data was then written back to an S3 bucket as a CSV file.<br>
- **Data Visualization**:<br>

	- Uploaded the final processed CSV file to Looker Studio.<br>
	- Created three charts to visualize the data:<br>
	- Trend of Score: Analyzed the trend of scores over time.
	- Metadata: Provided an overview of key metadata from the posts.
	- Most Scored by Flair: Visualized which flairs received the highest scores.

![image](https://github.com/user-attachments/assets/022bb968-e900-4941-99c8-5315ae8661c1)

The Looker Studio dashboard can be accessed here:
https://lookerstudio.google.com/reporting/c58f420a-e8e4-4bc8-b876-e6b487e9210d/page/qrBAE
