import flask

app = flask.Flask(__name__)
app.config["DEBUG"] = True


@app.route('/', methods=['GET'])
def home():
    return "<h1>Yieldify Exercise</h1><p>This API is to shwo the metrics calculated from the data.</p>"

@app.route('/api/v1/metrics', methods=['POST'])
def get_timestamp():
    return None

# A route to return all of the available entries in our catalog.
@app.route('/api/v1/metrics', methods=['GET'])
def metrics():
    return None


@app.errorhandler(404)
def page_not_found(e):
    return "<h1>404</h1><p>Page Not Found</p>", 404

app.run()