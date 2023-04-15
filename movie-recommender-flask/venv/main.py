from flask import Flask, jsonify, request, render_template, session
import mysql.connector
import sqlalchemy
from sqlalchemy import *
import pandas as pd
import json
from sentence_transformers import SentenceTransformer
import uuid

# Initialize connection.
# Uses Flask cache to only run once.
def init_connection():
    return mysql.connector.connect(**app.config["MYSQL"])

app = Flask(__name__)
app.config["MYSQL"] = {
    "host": "svc-a3f7f2ba-378c-40ab-be2e-9fa993172504-dml.aws-virginia-5.svc.singlestore.com",
    "user": "admin",
    "password": "Test1234",
    "database": "movie_recommender"
}
app.secret_key = "Test1234"

model = SentenceTransformer('flax-sentence-embeddings/all_datasets_v3_mpnet-base')

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/autocomplete', methods=["POST"])
def search_movies():
    try:
        data = request.get_json()
        prefix = data['input'] + "*"
        conn = init_connection()
        query = (
            "WITH queryouter AS ("
                "SELECT DISTINCT(title), movieId, MATCH(title) AGAINST (%s) as relevance "
                "FROM movies_with_full_text "
                "WHERE MATCH(title) AGAINST (%s) "
                "ORDER BY relevance DESC "
                "LIMIT 3"
            ")"
            "SELECT title, movieId FROM queryouter;"
        )
        cursor = conn.cursor()
        cursor.execute(query, (prefix, prefix))
        rows = cursor.fetchall()
        #results = [{"title": row[1], "movieId": row[0]} for row in rows]
        titles = [row[0] for row in rows]
        print(titles)
        return jsonify(titles)
        #return redirect(url_for('movie_recommendation', titles=','.join(titles)))
    except Exception as e:
        print(e)
        return "Error"

@app.route("/movie_recommendation", methods=["POST"])
def movie_recommendation():
    cursor = None
    try:
        # Get the titles from the query parameter
        selected_movies = request.json['selected_movies']
        # Use the titles to generate movie recommendations
        conn = init_connection()
        print(selected_movies)
        user_session = "user_" + str(uuid.uuid4()).replace("-", "")
        values_str = ', '.join(map(lambda movie: f"('{movie}')", selected_movies))
        print(values_str)

        # define the SQL statement to insert a row into the user_choice table
        sql_insert = "INSERT INTO user_choice (userid, title, ts) VALUES (%s, %s, now())"

        # define a list of tuples, where each tuple contains the values for a single row to be inserted
        movie_data = []
        for movie in selected_movies:
            movie_data.append((user_session, movie))

        # execute the insert statement for all the rows in one go
        cursor = conn.cursor()
        cursor.executemany(sql_insert, movie_data)
        conn.commit()

        print(conn)
        print(cursor)
        
        # Run the query you provided
        cursor.execute(f"""
                with table_match as (
                SELECT m.title, m.movieId, m.vector
                FROM user_choice t
                INNER JOIN movie_with_tags_with_vectors m on m.title = t.title
                where userid=%s
                ),
            movie_pairs AS (
                SELECT m1.movieId AS movieId1, m1.title as title1, m2.movieId AS movieId2, m2.title as title2, DOT_PRODUCT(m1.vector, m2.vector) AS similarity 
                FROM table_match m1 
                CROSS JOIN movie_with_tags_with_vectors m2
                WHERE m1.movieId != m2.movieId),
                movie_match as ( 
                    SELECT movieId1,title1, movieId2,title2, similarity 
                    FROM movie_pairs 
                    WHERE similarity > 0.4 
                    order by similarity desc), 
                        distinct_count as ( 
                            select distinct movieId2, title2 as Title, round(avg(similarity),2) as Rating_Match from movie_match 
                            group by movieId2,title 
                            order by Rating_Match desc)
                            SELECT Title, Rating_Match FROM distinct_count
                            order by Rating_Match desc
                            limit 5;
        """, (user_session,))

        # Fetch the result rows
        
        rows = cursor.fetchall()

        results = []
        for row in rows:
            result = {}
            result['title'] = row[0]
            result['rating_match'] = row[1]
            results.append(result)

        # now you can use the results list as needed
        print(results)

    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()

    df = pd.DataFrame(results, columns=["title", "rating_match"])
    print(df)
    #return render_template("index.html", data=df.to_html(classes="table table-striped table-hover"))

#NONE OF THE STUFF BELOW IS IN USE ... We will need a new route for when the submit button is hit....

@app.route("/", methods=["POST"])
def recommend_movies():
    # retrieve the form data
    query_sentence = request.form["query_sentence"]
    slider_value = request.form["slider_value"]
    preference = request.form['preference']
    if preference == 'select':
        user = request.form['user']
    else:
        user = None
    
    # store the preference value in the session
    session['preference'] = session.get('preference', 'add')
    
    conn = init_connection()
    
    df_intro = pd.read_sql('SELECT distinct(userId) as userId FROM ratings', con=conn)
    users = df_intro['userId'].tolist()
    users = users[:10]
    
    xq = model.encode(query_sentence).tolist()
    search_embedding = json.dumps(xq)
    
    sql_query1 = ("""With selected_movies as ( 
                SELECT movieId,title, genres, round(DOT_PRODUCT(vector, JSON_ARRAY_PACK(%s)),3) AS Score FROM movie_with_tags_with_vectors tv 
                order by Score DESC 
                limit 100), 
                movie_rating as ( 
                    SELECT r.movieId,title, genres, Score, round(AVG(r.rating),1) as avg_rating from selected_movies sm 
                    inner JOIN ratings AS r ON r.movieId = sm.movieId 
                    group by r.movieId,title, genres, Score), 
                    movie_avg_rating as ( 
                        select movieId, title, genres, Score, avg_rating from movie_rating 
                        where avg_rating>%s), 
                        table_user_preference as ( 
                            select movieId, rating, timestamp from ratings 
                            where userId = %s and rating >= 4 
                            order by timestamp desc 
                            limit 100), 
                                filter_vector as ( 
                                select mtv.* from movie_with_tags_with_vectors mtv 
                                inner join table_user_preference tup on tup.movieId = mtv.movieId), 
                                movie_pairs AS ( 
                                    SELECT m1.movieId AS movieId1, m1.title as title1, m2.movieId AS movieId2, m2.title as title2, 
                                    DOT_PRODUCT(m1.vector, m2.vector) AS similarity 
                                    FROM filter_vector m1 
                                    CROSS JOIN movie_with_tags_with_vectors m2 
                                    WHERE m1.movieId != m2.movieId), 
                                        movie_match as ( 
                                            SELECT movieId1,title1, movieId2,title2, similarity 
                                            FROM movie_pairs 
                                            WHERE similarity > 0.4 
                                            order by similarity desc), 
                                            distinct_count as ( 
                                                select distinct movieId2, title2, round(avg(similarity),2) as Rating_Match from movie_match 
                                                group by movieId2,title2 
                                                order by Rating_Match desc) 
                                                select title, genres,avg_rating, Score, Rating_Match from distinct_count 
                                                inner join movie_avg_rating on movie_avg_rating.movieId = distinct_count.movieId2 
                                                order by Rating_Match desc; """
                )
    
    sql_query3 = ("""WITH user_ratings AS (
                        SELECT r.movieID, r.userID, r.rating, m.genres
                        FROM ratings AS r
                        JOIN movie_with_tags_with_vectors AS m ON r.movieId = m.movieId
                        WHERE r.userId = %s
                    ),
                    genre_ratings AS (
                        SELECT SUBSTRING_INDEX(SUBSTRING_INDEX(t.genres, '|', n.n), '|', -1) AS genre, t.rating
                        FROM user_ratings AS t CROSS JOIN
                            (SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5) AS n
                        WHERE n.n <= CHAR_LENGTH(t.genres) - CHAR_LENGTH(REPLACE(t.genres, ',', '')) + 1
                    ),
                    top_3_genres AS (
                        SELECT genre, AVG(rating) AS avg_rating
                        FROM genre_ratings
                        GROUP BY genre
                        ORDER BY avg_rating DESC
                        LIMIT 3
                    )
                    SELECT GROUP_CONCAT(CONCAT(genre) SEPARATOR ' | ') as conc
                    FROM top_3_genres """
                    )
    
    sql_query4 = ("""select tup.title, tup.genres,r.rating from ratings r
                    inner join movie_with_tags_with_vectors tup on tup.movieId = r.movieId
                    where userId = %s and rating >= 4
                    order by timestamp desc
                    limit 10; """
                    )
    
    sql_query2 = ("""With selected_movies as ( 
                SELECT movieId,title, genres, round(DOT_PRODUCT(vector, JSON_ARRAY_PACK(%s)),3) AS Score FROM movie_with_tags_with_vectors tv 
                order by Score DESC 
                limit 100), 
                movie_rating as ( 
                SELECT title, genres, Score, round(AVG(r.rating),1) as avg_rating from selected_movies sm 
                inner JOIN ratings AS r ON r.movieId = sm.movieId 
                group by title, genres) 
                select title, genres, avg_rating,Score from movie_rating 
                where avg_rating>%s"""
                )
    if preference == 'select':
        try:
            cursor = conn.cursor()
            cursor.execute(sql_query1, (search_embedding, slider_value,user))
            output_list = cursor.fetchall()
            cursor.execute(sql_query3, (user,))
            output_list2 = cursor.fetchall()
            cursor.execute(sql_query4, (user,))
            output_list3 = cursor.fetchall()
        finally:
            cursor.close()
            conn.close()
            
        # Convert output_list to a pandas dataframe for easier display in a table
        df = pd.DataFrame(output_list, columns=["Title", "Genres", "avg_rating", "Score", "Rating_Match"])
        df = df.rename(columns={"avg_rating": "Movie Rating"})
        df = df.rename(columns={"Rating_Match": "Prior User Review Score"})
        df = df.rename(columns={"Score": "Similar Movie Score"})
        df['Similar Movie Score'] = df['Similar Movie Score'].round(3)
        df['Movie Rating'] = df['Movie Rating'].round(1)
        df['Prior User Review Score'] = df['Prior User Review Score'].round(2)
        df = df.sort_values('Similar Movie Score', ascending=False)  # sort by Score in descending order
        df.index += 1  # Change the index to start from 1 instead of 0
        df2 = pd.DataFrame(output_list2, columns=["Favorite Movie Genres"])
        favorites_genres = df2['Favorite Movie Genres'].tolist()
        df3 = pd.DataFrame(output_list3, columns=["Title", "Genres", "Rating"])
        return render_template("index.html", data=df.to_html(classes="table table-striped table-hover"),data2=df3.to_html(classes="table table-striped table-hover"), query_sentence=query_sentence, slider_value=slider_value,user=user,preference=preference,favorites_genres=favorites_genres)

    else:
        try:
            cursor = conn.cursor()
            cursor.execute(sql_query2, (search_embedding, slider_value))
            output_list = cursor.fetchall()
        finally:
            cursor.close()
            conn.close()
            
        # Convert output_list to a pandas dataframe for easier display in a table
        df = pd.DataFrame(output_list, columns=["Title", "Genres", "avg_rating", "Score"])
        df = df.rename(columns={"avg_rating": "Movie Rating"})
        df = df.rename(columns={"Score": "Similar Movie Score"})
        df['Similar Movie Score'] = df['Similar Movie Score'].round(3)
        df['Movie Rating'] = df['Movie Rating'].round(1)
        df = df.sort_values('Similar Movie Score', ascending=False)  # sort by Score in descending order
        df.index += 1  # Change the index to start from 1 instead of 0

        return render_template("index.html", data=df.to_html(classes="table table-striped table-hover"), query_sentence=query_sentence, slider_value=slider_value,users=users)

if __name__ == "__main__":
    app.run(debug=True, port=5000)
