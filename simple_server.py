import gevent.monkey

gevent.monkey.patch_all()

from flask import Flask, request
from celery import Celery
from flaskext.mysql import MySQL
from flask_sqlalchemy import SQLAlchemy
import datetime
import grequests

app = Flask(__name__)
mysql = MySQL()

app.config['CELERY_BROKER_URL'] = 'amqp://localhost'
app.config['CELERY_RESULT_BACKEND'] = 'rpc://'

celery = Celery(app.name, broker=app.config['CELERY_BROKER_URL'])
celery.conf.update(app.config)

app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql+pymysql://root:admin@localhost/sample_server'
db = SQLAlchemy(app)


def get_all_records():
    """
    This method fetches all the records in the table 'datadump' and returns it
    :return:
    """
    try:
        cursor = db.session.execute("SELECT * from datadump;")
        data = cursor.fetchall()

        return data
    except Exception as e:
        print("error: " + str(e))
        return "ERROR"


def insert_record(data, url):
    """
    This method inserts the data and url in the table 'datadump'
    :param data: the payload data
    :param url: the url of the resource being hit
    :return:
    """
    try:
        print("inside insert_record")
        print("data: " + str(data))
        print("url: " + str(url))

        db.session.execute(
                "Insert into datadump(request_data, request_url, created_at) VALUES ('" + data[
                    "data"] + "', '" + url + "', '" + datetime.datetime.now().strftime(
                        '%Y-%m-%d %H:%M:%S') + "')")
        db.session.commit()

        return True
    except Exception as e:
        print("Error: " + str(e))
        return None


URLS = ["http://requestb.in/1c2shtm1",
        "http://mockbin.org/bin/b62b4b9f-826e-43c5-a1c4-61a47b0503ee"]


@celery.task
def hit_and_store_db(payload):
    """
    This is the task which is added to the celery queue.
    :param payload:
    :return:
    """
    rs = (grequests.post(u, data=payload) for u in URLS)
    print "Requests queued up"
    result = grequests.map(rs)
    for response in result:
        print str(response.status_code) + str(response.url)
        insert_record(payload, response.url)
        print("record inserted")


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
