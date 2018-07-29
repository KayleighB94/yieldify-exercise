import sqlite3
from flask import Flask, render_template, request
import pandas as pd

app = Flask(__name__)
app.config["DEBUG"] = True


@app.route('/', methods=['GET'])
def home():
    return "<h1>Yieldify Exercise</h1><p>This API is to show the metrics calculated from the data.</p>"

@app.route('/stats/<string>:type', methods=['GET'])
def all_metrics(type):
    #type="browser"
    # Getting the data
    conn = sqlite3.connect('data.db')
    sql_query = 'select '+type+', count(*) as count from web_data group by '+type
    df = pd.read_sql(sql_query, con=conn)
    total = df["count"].sum()
    df["percentage"] = df["count"].apply(lambda v: (v/total)*100)
    df = df.sort_values("percentage", ascending=False).reset_index()[[type, "count", "percentage"]]
    return render_template("percentage.html", type=type, data=df.to_html(), total=total)


@app.errorhandler(404)
def page_not_found(e):
    return "<h1>404</h1><p>Page Not Found</p>", 404

app.run()