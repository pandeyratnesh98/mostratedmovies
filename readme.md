<h1>Most Rated Movies Pipeline from 25 million ratings</h1>
<p>In this Project <br>
  * I've downloaded the movies data zip file from http://files.grouplens.org/datasets/movielens/ml-25m.zip and exposed each csv file to a different node using flask api(movierec-api).<br>
  * In airflow, I've created three tasks:<br>
    1. fetch_each_movieapi_node:<br>
       Fetches data from flask API and dumps it into JSON, <br>
    2. get_avg_ratings:<br>
       Creates a data frame and cleans redundant release year in the title and adds it to a separate column relase_year. At the same time, In the second task, I calculated average ratings and the number of users rated for the particular movie.<br>
    3. upload_movies_data_to_s3:<br>
       Save the JSON data into the AWS S3 bucket<br>
  * 4. In airflow DAG, upload_movies_data_to_s3 is dependent on get_avg_ratings and get_avg_ratings  is dependent on fetch_each_movieapi_node.</p>

      fetch_each_movieapi_node >> get_avg_ratings >> upload_movies_data_to_s3

  
  <h3>Running this project project</h3>

Clone this project:
    
    git clone https://github.com/pandeyratnesh98/mostratedmovies.git

To get started with the code examples, start Airflow in docker using the following command:

    docker-compose up -d --build

Wait for a few seconds and you should be able to access the examples at http://localhost:8080/.

Add amazon s3 connection:

    Airflow Home > Admin > Connections 
    click on plus sign add connectiontype aws providing your access_keey and secret_key

To stop running the examples, run the following command:

    docker-compose down


  
