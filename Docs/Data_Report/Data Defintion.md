# Data and Feature Definitions

This document provides a central hub for the raw data sources, the processed/transformed data, and feature sets. More details of each dataset is provided in the data summary report. 

For each data, an individual report describing the data schema, the meaning of each data field, and other information that is helpful for understanding the data is provided. If the dataset is the output of processing/transforming/feature engineering existing data set(s), the names of the input data sets, and the links to scripts that are used to conduct the operation are also provided. 


For each dataset, the links to the sample datasets in the _**Data**_ directory are also provided. 



## Raw Data Sources


| Dataset Name | Original Location   | Destination Location  | Data Movement Tools / Scripts | Link to Report |
| ---:| ---: | ---: | ---: | -----: |
|University_degrees | This dataset was provided by UEX on April 18, 2017 | This dataset it is located in us [repository](https://drive.google.com/drive/folders/1Osm27prcsBejYoVYSSPlFefSm_SJ6dG2?usp=sharing) of Google Drive| Google Drive IU | [University_degrees_Report](-)|
|Prev_Education | This dataset was provided by client via mail on April 17, 2018 | This dataset it is located in us [repository](https://drive.google.com/drive/folders/1Osm27prcsBejYoVYSSPlFefSm_SJ6dG2?usp=sharing) of Google Drive| Google Drive IU | [Prev_education Report](./Raw/Prev_education-DataSummaryReport.md)|
|Work_history | This dataset was provided by client via mail on April 17, 2018 | This dataset it is located in us [repository](https://drive.google.com/drive/folders/1Osm27prcsBejYoVYSSPlFefSm_SJ6dG2?usp=sharing) of Google Drive|Google Drive IU| [Work_history Report](./Raw/Work_History-DataSummaryReport.md)|
|Training_courses | This dataset was provided by client via mail on April 17, 2018 | This dataset it is located in us [repository](https://drive.google.com/drive/folders/1Osm27prcsBejYoVYSSPlFefSm_SJ6dG2?usp=sharing) of Google Drive| Google Drive IU | [Training_courses_Report](./Raw/Training_courses-DataSummaryReport.md)|
|Languages_certs | This dataset was provided by client via mail on April 17, 2018 | This dataset it is located in us [repository](https://drive.google.com/drive/folders/1Osm27prcsBejYoVYSSPlFefSm_SJ6dG2?usp=sharing) of Google Drive| Google Drive IU | [Language_certs Report](./Raw/Languages_certs-DataSummaryReport.md)|

* **University_degrees:** this dataset provides information about all people that studied a university degree from 1976 to 2018. Their columns description can found in this [link](../Data_Dictionaries/university_degrees-data_dictionary.xlsx).
* **Prev_Education:** this dataset provides information about all the students from higher and secondary education. Their columns description can found in this link [link](../Data_Dictionaries/prev_education-data_dictionary.xlsx)
* **Work_history:** this dataset provides information about all contracts of jobs register from 1990 to 2018. Their columns description can found in this [link](../Data_Dictionaries/work_history-data-dictionary.xlsx).
* **Training_courses:** this dataset provides information about all people that studied any formation course offer by client from 1993 to 2018. Their columns description can found in this [link](../Data_Dictionaries/training_courses-data-dictionary.xlsx).
* **Languages_certs:** this dataset provides information about all the university students that have any certification of any language certified with School of Languages of Spain. Their columns description can found in this link [link](../Data_Dictionaries/languages_certs-data_dictionary.xlsx)


## Processed Data
| Processed Dataset Name | Input Dataset(s)   | Data Processing Tools/Scripts | Link to Report |
| ---:| ---: | ---: | ---: | 
| pr_univ_degrees| [University_degrees](-) |[etl_univ_degrees.py](../../Code/Data_Acquisition_and_Understanding/ETL/etl_univ_degrees/etl_univ_degrees.py) | -|
| pr_prev_education| [Previous_Education](./Raw/Prev_education-DataSummaryReport.md) |[etl_prev_education.py](../../Code/Data_Acquisition_and_Understanding/ETL/etl_prev_education/etl_prev_education.py) | -|
| pr_work_history| [Work_history](./Raw/Work_History-DataSummaryReport.md) |[etl_work_history.py](../../Code/Data_Acquisition_and_Understanding/ETL/etl_work_history/etl_work_history.py) | - |
| pr_training_courses| [Training_courses](./Raw/Training_courses-DataSummaryReport.md) |[etl_training_courses.py](../../Code/Data_Acquisition_and_Understanding/ETL/etl_training_courses/etl_training_courses.py) | - |
| pr_language_certs| [Languages_certs](./Raw/Languages_certs-DataSummaryReport.md) |[etl_langs_certs.py](../../Code/Data_Acquisition_and_Understanding/ETL/etl_langs_certs/etl_langs_certs.py) | -|
| abt_univ_degrees | pr_univ_degrees, pr_prev_education, pr_work_history, pr_training_courses, pr_language_certs | [int_univ_degrees](../../Code/Data_Acquisition_and_Understanding/Integration/int_univ_degrees/int_univ_degrees.py) |[abt_univ_degrees_Report](./Integrated/abt_univ_degrees-DataSummaryReport.md)|

* **pr_univ_degrees:** this dataset provides information about all people that studied a university degree from 1976 to 2018.
* **pr_previous_education:** this dataset provides information about all the students from higher and secondary education.
* **pr_work_history:** this dataset provides information about all contracts of jobs register from 1990 to 2018.
* **pr_training_courses:** this dataset provides information about all people that studied any formation course offer by client from 1993 to 2018.
* **pr_language_certs:** this dataset provides information about all people that have acquired a title of language  through School of Languages of Spain.

## Feature Sets

| Feature Set Name | Input Dataset(s)   | Feature Engineering Tools/Scripts | Link to Report |
| ---:| ---: | ---: | ---: | 
| anlys_univ_degrees | [abt_univ_degrees](./Integrated/abt_univ_degrees-DataSummaryReport.md)| [feat_eng_abt_univ_degrees.py](link/to/R/script/file/in/Code) | [anlys_univ_degrees](./Feature Sets/analys_univ_degrees-DataSummaryReport.md)|

* **anlys_univ_degrees summary:** This dataset contains the information that has really influenced the percentage worked, which is the variable to be analysed later on.

