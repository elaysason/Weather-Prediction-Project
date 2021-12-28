# Weather-Prediction-Project
In this project, we get the data from Kafka stream and use it to predict the precipitation across the globe. 

1. [General](#General)
    - [Background](#background)
    - [Program Structure](https://github.com/elaysason/Deep-Learning-Comparining-Overfitting/blob/main/README.md#program-structure)
    - [Running Instructions](https://github.com/elaysason/Deep-Learning-Comparining-Overfitting/blob/main/README.md#running-instructions)
2. [Installation](#installation)
3. [Footnote](#footnote)

## General
 The project is fouced on record from diffrent weather stations all across Europe.

### Program Structure
The order of files is:
* ETL(extract,transform load) - In this notbook we got the data using kafka stream and transfomed it to our liking and into our desired and according to our deisgn into the diffrent tables.We chose to have 3 tables: time_df,sprtial_df and model_df.Time and sprital was used in our analsis according to time and sprtial of the data and model was used to train the model and comabines them both.Specific features were chosen also to our analsis and the models.
a. Data Analsis - We analyzed the data in 3 diffrents ways in order to select the right features for the model:
    1. Time based - We found that there is a relation between the diffrent months and the varience of the perception in those months.
    2. Location based - We tried in first to find a realtion by country but we seen that the differnce between the diffrent countries isn't big enough.Therefore using            kmean algorithm we created 4 diffrent clusters(where each one serves as diffrent climate zone in Europe) 
    3. Time and location based - In this part we have looked at the sum of precipitation across the diffrent clustrs over the years and seen that the precipitation is 
       diffrent from cluster to cluster and that the order ration between the clusters remains the same most of the time.
b. MLmodel - Our goal in this part is given station and day to predict the precipitation amount in that day. We chose gradient boosting based tree after comapring with 
   random forset.
c. 
