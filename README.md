# Weather-Prediction-Project
In this project, we get the data from Kafka stream and use it to predict the precipitation across the globe. 

1. [General](#General)
    - [Program Structure](https://github.com/elaysason/Weather-Prediction-Project/blob/main/README.md#program-structure)  
2. [Installation](#Installation)
4. [Footnote](#footnote)

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
c. Bouns - In this part we fouced on pattren recoginission. After divining the data into days with exterme amount of precipitation, days with normal amount of       
   precipitation and days without precipitation we chose it as our target variable.We chose naive bayesian classification becasue we saw that there isnt a simple relation    between the features and the relation is given a feature the probiality for the target parmater is higher which bayesian model is fitted to.
### Installation
1.Open the terminal
2.Clone the project by:
    $ git clone https://github.com/tomershay100/Multiclass-Classification.git
### Footnote
I didn't include a running guide as the server which the data was taken from is no longer available so running the code in your machine is problmatic. I uploaded the project to present the procces and the results.
