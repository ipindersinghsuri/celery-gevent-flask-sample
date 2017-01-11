from flask import Flask, render_template, request, url_for, jsonify
from celery import Celery
import requests
import gevent
import json
from flaskext.mysql import MySQL
import datetime

app = Flask(__name__)
mysql = MySQL()

app.config['CELERY_BROKER_URL'] = 'amqp://localhost'
app.config['CELERY_RESULT_BACKEND'] = 'rpc://'

celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)

app.config['MYSQL_DATABASE_USER'] = 'root'
app.config['MYSQL_DATABASE_PASSWORD'] = 'admin'
app.config['MYSQL_DATABASE_DB'] = 'sample_server'
app.config['MYSQL_DATABASE_HOST'] = 'localhost'
mysql.init_app(app)


def get_all_records():
    """
    This method fetches all the records in the table 'datadump' and returns it
    :return:
    """
    cursor = mysql.connect().cursor()
    cursor.execute("SELECT * from datadump;")
    data = cursor.fetchall()
    return data


def insert_record(data, url):
    """
    This method inserts the data and url in the table 'datadump'
    :param data: the payload data
    :param url: the url of the resource being hit
    :return:
    """
    try:
        conn = mysql.connect()
        cursor = conn.cursor()
        cursor.execute(
                "INSERT INTO datadump(request_data, request_url, created_at) values(%s, %s, %s)",
                (data["data"], url, datetime.datetime.now()))
        conn.commit()
        return True
    except Exception as e:
        print("Error: " + str(e))
        return None


REQUESTBIN_URL = "http://requestb.in/1c2shtm1"
MOCKBIN_URL = "http://mockbin.org/bin/b62b4b9f-826e-43c5-a1c4-61a47b0503ee"


def worker(url, payload):
    """
    The gevent worker which performs the task
    :param url:
    :param payload:
    :return:
    """
    response = requests.request("POST", url, data=json.dumps(payload))
    print("url " + str(url) + " - " + str(response.status_code))
    insert_record(payload, url)


@celery.task
def hit_and_store_db(payload):
    """
    This is the task which is added to the celery queue.
    :param payload:
    :return:
    """
    threads = list()
    threads.append(gevent.spawn(worker, REQUESTBIN_URL, payload))
    threads.append(gevent.spawn(worker, MOCKBIN_URL, payload))
    gevent.joinall(threads)


@app.route('/')
def index():
    """
    A view which return the string of all the records. Just for demonstration purpose
    :return:
    """
    all_records = get_all_records()

    return str(all_records)


@app.route('/hello', methods=["POST"])
def hello():
    """
    The API end point for sending the data.
    Sample input:
    {
        "data":"This is a sample request."
    }
    :return:
    """
    input_json = request.get_json(force=True)
    # force=True, above, is necessary if another developer
    # forgot to set the MIME type to 'application/json'
    print 'data from client:', input_json

    hit_and_store_db.delay(input_json)

    """
    We are sending 202, because the request has been accepted but processing is not complete yet.
    """
    return "", 202


if __name__ == "__main__":
    app.run()
