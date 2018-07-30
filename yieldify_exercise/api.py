import datetime
import sqlite3
from flask import Flask, render_template, request
import pandas as pd

app = Flask(__name__)
app.config["DEBUG"] = True


@app.route('/stats/<object_type>', methods=['GET'])
def all_metrics(object_type):
    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")

    # Setting up a connection to the database
    conn = sqlite3.connect('data.db')

    # Constructing the SQL query to get the data
    sql_query = 'select '+object_type+', count(*) as count from web_data '
    if (start_date is not None) & (end_date is not None):
        sql_query = sql_query + 'where timestamp between ' + start_date + ' and ' + end_date + ' group by ' + object_type
    elif start_date is not None:
        sql_query = sql_query + 'where timestamp > '+start_date+' group by '+object_type
    elif end_date is not None:
        sql_query = sql_query + 'where timestamp < ' + end_date + ' group by ' + object_type
    else:
        sql_query = sql_query + 'group by ' + object_type
    df = pd.read_sql(sql_query, con=conn)

    # Finding the total amount
    total = df["count"].sum()
    # Calculating the percentage usage
    df["percentage"] = df["count"].apply(lambda v: (v/total)*100)
    # Re-ordering to show the one used the most first
    df = df.sort_values("percentage", ascending=False).reset_index()[[object_type, "count", "percentage"]]
    return render_template("percentage.html", object_type=object_type, data=df.to_html(), total=total, start_date=start_date,
                           end_date=end_date)


@app.errorhandler(404)
def page_not_found(e):
    return "<h1>404</h1><p>Page Not Found</p>", 404

app.run()