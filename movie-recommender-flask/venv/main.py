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
            WITH table_match AS (
                    SELECT m.title, m.movieId, m.vector
                    FROM user_choice t
                    INNER JOIN movie_with_tags_with_vectors m ON m.title = t.title
                    WHERE userid = %s
            ),
            movie_pairs AS (
                    SELECT m1.movieId AS movieId1, m1.title AS title1, m2.movieId AS movieId2, m2.title AS title2, DOT_PRODUCT(m1.vector, m2.vector) AS similarity 
                    FROM table_match m1 
                    CROSS JOIN movie_with_tags_with_vectors m2
                    WHERE m1.movieId != m2.movieId
                    AND NOT EXISTS (
                        SELECT 1
                        FROM user_choice uc
                        WHERE uc.userid = %s
                        AND uc.title = m2.title
                    )
            ),
            movie_match AS ( 
                    SELECT movieId1, title1, movieId2, title2, similarity 
                    FROM movie_pairs 
                    ORDER BY similarity DESC
            ), 
            distinct_count AS ( 
                    SELECT DISTINCT movieId2, title2 AS Title, ROUND(AVG(similarity), 2) AS Rating_Match 
                    FROM movie_match 
                    GROUP BY movieId2, title2
                    ORDER BY Rating_Match DESC
            )
            SELECT Title, Rating_Match 
            FROM distinct_count
            ORDER BY Rating_Match DESC
            LIMIT 5;
        """, (user_session, user_session))

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
    print(jsonify(results))
    print(type(results))
    return(jsonify(results))

    df = pd.DataFrame(results, columns=["title", "rating_match"])
    print(df)
    #return render_template("index.html", data=df.to_html(classes="table table-striped table-hover"))

if __name__ == "__main__":
    app.run(debug=True, port=5000)
