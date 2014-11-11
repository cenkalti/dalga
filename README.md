Dalga
=====

[![Build Status](https://travis-ci.org/cenkalti/dalga.png)](https://travis-ci.org/cenkalti/dalga)

Dalga is a job scheduler.

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

Run
---

All configuration is done with command line flags:


    $ dalga -h
    Usage dalga:
      -create-table=false: create table for storing jobs
      -debug=false: turn on debug messages
      -http-host="0.0.0.0":
      -http-port="17500":
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

HTTP API
--------

Schedule a new job to run every 60 seconds:

    $ curl -i -X PUT 'http://127.0.0.1:34006/jobs/check_feed/1?interval=60'
    HTTP/1.1 201 Created
    Date: Tue, 11 Nov 2014 17:25:08 GMT
    Content-Length: 0
    Content-Type: text/plain; charset=utf-8

Update the interval of existing job:

    $ curl -i -X PUT 'http://127.0.0.1:34006/jobs/check_feed/1?interval=15'
    HTTP/1.1 204 No Content
    Date: Tue, 11 Nov 2014 17:26:02 GMT

Cancel previously scheduled job:

    $ curl -i -X DELETE 'http://127.0.0.1:34006/jobs/check_feed/1'
    HTTP/1.1 204 No Content
    Date: Tue, 11 Nov 2014 17:26:50 GMT
