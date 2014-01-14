Dalga
=====

A periodic message publishing server.

Dalga is designed to be a job scheduler and
provides a specialized way of scheduling jobs.

It consists of a web server and a publisher thread.
While web server provides methods for scheduling and cancelling jobs,
publisher is responsible for reading the stored jobs from database
and publishing them to a RabbitMQ exchange.

It is a big improvement from polling on database and very efficient.

Requirements
------------

* RabbitMQ
* MySQL

Usage
-----

First, you need a config file. Please see ``dalga.ini`` file from the repository.

To create a table for storing jobs::

    $ dalga -c dalga.ini -t

Then, run the server::

    $ dalga -c dalga.ini

By default it listens on ``0.0.0.0:17500``. There are two methods of server.
Use them to schedule new jobs and cancel existing jobs.

```
/schedule?routing_key=key&body=body&interval=5
    Schedules a new job to run on every 5 seconds.

/cancel?routing_key=key&body=body
    Cancels previously scheduled job.
```
