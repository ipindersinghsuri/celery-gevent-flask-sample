# celery-gevent-flask-sample
A sample project for understanding the basic concepts for flask, celery and gevent and their working together.

# Use case

We need to create a flask server which accepts request at an end-point and queues it to celery. Celery tasks are performed via gevent which hits the data at two urls and stores the information in the database. A root end point is created which lists all the data in the table.