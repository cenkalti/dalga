Dalga
=====

A periodic message publishing server.

Dalga is designed to be a job scheduler and
provides a specialized way of scheduling jobs.

It consists of a web server and a message publisher thread.
While web server provides methods for scheduling and canceling jobs,
publisher is responsible for reading the stored jobs from database
and publishing them to a RabbitMQ exchange.

It is a big improvement from polling on database and very efficient.

Let me explain with an example: Suppose that you have 1,000,000 RSS feed
that you want to check for updates in every 5 minutes. Also, each feed has
different check time (i.e. feed A must be checked at t, t+5, t+10 and
feed B must be checked at t+2, t+7, t+12). The naive approach is selecting
the feeds which their time has come with a query like
`SELECT * FROM rss WHERE check_time < NOW()` and iterate over the results and
process them one by one. Since the check operation may take a long time
(slow server, timeouts, vs...), you may want to distribute those checks to
your servers. Then a message broker comes into the scene and you publish a
message for each feed that needs to be checked. Dalga replaces the first part.
Instead of running a custom script to distribute those check operations,
you schedule each feed in Dalga with a routing key, a message body
and an interval that you want. Then, Dalga will publish that messages at the
specified intervals. In this example the routing key may be "rss" so you point
your workers to consume messages from "rss" queue and the body of the message
may be the serialized representation of the feed.

Currently the messages are fetched from the database one-by-one but it can
be changed to fetch in chunks to reduce query load on the database in future.

Requirements
------------

* RabbitMQ
* MySQL

Install
-------

    $ go get github.com/cenkalti/dalga

Usage
-----

First, you need a config file.
You can use ``dalga.ini`` file from the repository as a template.

To create a table for storing jobs:

    $ dalga -c dalga.ini -t

Then, run the server:

    $ dalga -c dalga.ini

By default it listens on ``0.0.0.0:17500``. There are two methods of server.
Use them to schedule new jobs and cancel existing jobs.

```
/schedule?routing_key=key&body=body&interval=5
    Schedules a new job to run on every 5 seconds.

/cancel?routing_key=key&body=body
    Cancels previously scheduled job.
```
