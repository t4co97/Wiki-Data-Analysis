# Wiki Data Analysis



### Project Description
This projects analysis consists of using big data tools to answer questions about datasets from Wikipedia. There are a series of basic analysis questions, answered using Hive or MapReduce. The tool(s) used are determined based on the context for each question. The output of the analysis includes MapReduce jarfiles and/or .hql files so that the analysis is a repeatable process that works on a larger dataset, not just an ad hoc calculation. Assumptions and simplfications are required in order to answer these questions, and the final presentation of results includes a discussion of those assumptions/simplifications and the reasoning behind them. In addition to answers and explanations, this project requires a discussion of any intermediate datasets and the reproduceable process used to construct those datasets. Finally, in addition to code outputs, this project requires a simple slide deck providing an overview of results. The questions follow: 1. Which English wikipedia article got the most traffic on October 20, 2020? 2. What English wikipedia article has the largest fraction of its readers follow an internal link to another wikipedia article? 3. What series of wikipedia articles, starting with Hotel California, keeps the largest fraction of its readers clicking on internal links? 4. Find an example of an English wikipedia article that is relatively more popular in the UK, then find the same for the US and Australia. 5. How many users will see the average vandalized wikipedia page before the offending edit is reversed? 6. Run an analysis you find interesting on the wikipedia datasets we're using.

### Technologies Used
- HDFS
- YARN
- MapReduce
- Hadoop
- Hive
### Features
 - Creates Multiple tables and Views
 ### To-do 
 - Merge some tables so there are not as many
### Getting Started
- Download Hadoop, Yarn, Hive
- Setup cluster,hdfs, and Hive connection
- Download all relevant data from the wikipedia links below:
https://dumps.wikimedia.org/other/pageviews/readme.html
https://dumps.wikimedia.org/other/mediawiki_history/readme.html
https://dumps.wikimedia.org/other/clickstream/readme.html

Git Clone:
```
gh repo clone t4co97/Revature/WikiAnalysis
or
$git clone https://github.com/t4co97/Revature.git WikiAnalysis
```
### Usage
- Open up the script and comment out drop statements
- Run query
- or run each query individually and skip drop statements

