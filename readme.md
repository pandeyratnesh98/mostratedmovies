<h1>Most Rated Movies Pipeline from 25 millions ratings</h1>
<p>In this Project <br>
  1.I've downloaded the movies data zip file from http://files.grouplens.org/datasets/movielens/ml-25m.zip and expose each csv file to different node using flask api(movierec-api).<br>
  2. In airflow, I've created two tasks fetch_each_movieapi_node which fetched data from flask api and dump it into json and second task is get_avg_ratings which creates dataframe and clean redundant release year in title and add it to separate column relase_year. At the same time in second task I've calculated average rate and number of users rated for the particular movie.<br>
  3. In airflow DAG, get_avg_ratings is dependent on fetch_each_movieapi_node.</p>
  
  <h3>Running this project project</h3>

Clone this project:
    
    git clone https://github.com/pandeyratnesh98/mostratedmovies.git

To get started with the code examples, start Airflow in docker using the following command:

    docker-compose up -d --build

Wait for a few seconds and you should be able to access the examples at http://localhost:8080/.

To stop running the examples, run the following command:

    docker-compose down
  
