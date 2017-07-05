from flask import Flask, request, abort, render_template, json
from db import get_company_data, get_historical_data, get_companies

app = Flask(__name__, static_folder='dist')


@app.route('/')
def main_page():
    return render_template('index.html')


@app.route('/info/<ticker>')
def info_data(ticker):
    return render_template('infopanel.html', data=get_company_data(ticker))


@app.route('/details/<ticker>')
def detail_page(ticker):
    current = get_company_data(ticker)
    history = get_historical_data(ticker)
    return render_template('details.html', ticker=ticker, current=current, history=history)


@app.route('/data/companies')
def get_companies_data():
    data = get_companies()
    return json.jsonify(data)


@app.route('/data/history/<ticker>')
def get_history(ticker):
    date_range = request.args.get('range')
    data_type = request.args.get('type')
    if data_type:
        data = get_historical_data(ticker, data_type, date_range)
        return json.jsonify(data)
    else:
        abort(404)


if __name__ == '__main__':
    app.run(debug=True)
