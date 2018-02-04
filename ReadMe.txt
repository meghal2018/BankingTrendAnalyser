**README FILE**

Banking Trend Analyzer for ISTM 622 Advanced Data Management
by, 
	Team 6
	December 07, 2017

CONTENTS
I.		BUSINESS USE CASE
II.		MINIMUM SYSTEM REQUIREMENTS
III.	SOFTWARE/PLUG-IN DOWNLOADS
IV.		KNOWN ASSUMPTIONS ISSUES AND WORKAROUNDS

I.	BUSINESS USE CASE

With growing competition and lack of standardization in the banking industry, customer satisfaction is an
important attribute for any bank. Providing customer with what they want in a convenient manner is not
straightforward in most cases. To address this issue, we aim to come up with a solution that does trend
analysis of the customer complaints data set and try to identify root cause of the issues. As a result, our
system would be able to identify if the customer complaints for a banking product are reducing or increasing for each month.
We will also identify the hot spots in the current services of the bank, advise solutions to
improve response time for any complaint, thus improving the customer satisfaction.

II. 	CHANGES
Release Candidate-2:
	� Separated the classes to form new packages dataHandler, functionalModule.
	� Added data validations for all the inputs of the system. 
Release Candidate:
	� Added a new feature which determines the preferred sequence of mode of customer support for a particular product.
	� Re-factored the previous two functionalities to be more efficient, shifting major computation from hash map to map-reduce provided by hadoop. 
	
Beta Release:
	Added a new feature which analyzes rating to cost ratio for all the functionalities of a product for a given month. It gives us the following information:
		� Finding the area which is best handled by customer service team
		� For all other functionalities, it compares the cost and rating from the average value for respective product
Alpha Release:
	� Implementation includes the business case of identifying increment/reduction in customer complaints for a banking product
	� It uses two Map-Reduce functions on CustomerProductData.csv and ProductComplaints.csv to create a measure of product stability


III.	MINIMUM SYSTEM REQUIREMENTS

� Windows 7 or higher or Mac 9.2 and higher
� Pentium 366 MHz or higher 
� Minimum 4 GB of RAM
� Screen resolution 800x600 or higher

IV.	SOFTWARE/PLUG-IN DOWNLOADS
Hadoop version 2.8.1
Eclipse Oxy
Gradle 4.2.1

V.	KNOWN ASSUMPTIONS	
� Profile and Port Number for the  Hadoop instance are fs.default.name, 8020 respectively
� Refresh the project to get the result after running gradle application run task
� Company standard for complaint to customer(s) ratio is 0.1
� The values are hard coded now, but in subsequent versions in future, the inputs would be taken from the user
� No product or feature of a product contain a "_" in its name
� The sample data contains data between 1/13 to 7/13 months. Any other month is out of the range for the sample data. 

VI.	RUNNING INSTRUCTIONS
� For a first time run, no additional steps are needed
� For running the project multiple times, change the value of args[] at positions 3,4,5 in main method of CustomMapReduce.java class