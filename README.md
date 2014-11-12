Dalga
=====

[![Build Status](https://travis-ci.org/cenkalti/dalga.png)](https://travis-ci.org/cenkalti/dalga)

Dalga is a job scheduler.

- Can schedule periodic or one-off jobs.
- Stores jobs in a MySQL table.
- Has an HTTP interface for scheduling and cancelling jobs.
- Can schedule jobs periodically or one-off.
- Publishes the job's description to a RabbitMQ exchange on the job's scheduled time.

Rationale
---------

It is easier to explain with an example: Suppose that you have 1,000,000 RSS
feeds that you want to check for updates in every 5 minutes. Also, each feed
has different check time (i.e. feed A must be checked at t, t+5, t+10 and
feed B must be checked at t+2, t+7, t+12).

The naive approach is selecting the feeds
which their time has come every minute with a query like
`SELECT * FROM feeds WHERE check_time < NOW()` and iterate over the results
and process them one by one (presumably as a cron job).
Since this operation may take a long time
(slow server, timeouts, vs...), you may want to distribute those checks over
multiple servers. Then a message broker comes into the scene, you publish a
message for each feed that needs to be checked for updates.

By using Dalga, instead of running a custom script to distribute those check
operations, you schedule each feed in Dalga with a routing key, a message body
and an interval. Then, Dalga will publish job descriptions to a RabbitMQ
exchange at their intervals.
In feeds example, the routing key may be "check_feed" so you point
your workers to consume messages from "check_feed" queue and the body of the
message may contain the feed's ID and URL.

I think it is a big improvement over polling on database.

Install
-------

    $ go get github.com/cenkalti/dalga

Usage
-----

All configuration is done with command line flags:

    $ dalga -h
    Usage dalga:
      -create-table=false: create table for storing jobs
      -debug=false: turn on debug messages
      -http-host="127.0.0.1":
      -http-port="34006":
      -mysql-db="test":
      -mysql-host="localhost":
      -mysql-password="":
      -mysql-port="3306":
      -mysql-table="dalga":
      -mysql-user="root":
      -rabbitmq-exchange="":
      -rabbitmq-host="localhost":
      -rabbitmq-password="guest":
      -rabbitmq-port="5672":
      -rabbitmq-user="guest":
      -rabbitmq-vhost="/":

To create the table for storing jobs:

    $ dalga -create-table

Then, run the server:

    $ dalga

Schedule a new job to run every 60 seconds:

    $ curl -i -X PUT 'http://127.0.0.1:34006/jobs/check_feed/1234?interval=60'
    HTTP/1.1 201 Created
    Content-Type: application/json; charset=utf-8
    Date: Tue, 11 Nov 2014 22:10:40 GMT
    Content-Length: 89

    {"job":"1234","routing_key":"check_feed","interval":60,"next_run":"2014-11-11T22:11:40Z"}

60 seconds later, Dalga publishes a message to RabbitMQ server:

    Routing Key: check_feed
    Properties:
        expiration: 60000
        delivery_mode:  2
    Payload: 1234

Get the status of a job:

    $ curl -i -X GET 'http://127.0.0.1:34006/jobs/check_feed/1234'
    HTTP/1.1 200 OK
    Content-Type: application/json; charset=utf-8
    Date: Tue, 11 Nov 2014 22:12:21 GMT
    Content-Length: 89

    {"job":"1234","routing_key":"check_feed","interval":60,"next_run":"2014-11-11T22:12:41Z"}

Cancel previously scheduled job:

    $ curl -i -X DELETE 'http://127.0.0.1:34006/jobs/check_feed/1234'
    HTTP/1.1 204 No Content
    Date: Tue, 11 Nov 2014 22:13:35 GMT

Set `one-off=true` to schedule a one-off job:

    $ curl -i -X PUT 'http://127.0.0.1:34006/jobs/check_feed/1234?interval=60&one-off=true'
    HTTP/1.1 201 Created
    Content-Type: application/json; charset=utf-8
    Date: Wed, 12 Nov 2014 08:53:21 GMT
    Content-Length: 88

    {"job":"1234","routing_key":"check_feed","interval":0,"next_run":"2014-11-12T08:54:21Z"}
