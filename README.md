Dalga
=====

[![Build Status](https://travis-ci.org/cenkalti/dalga.png)](https://travis-ci.org/cenkalti/dalga)

Dalga is a job scheduler.

- Can schedule periodic or one-off jobs.
- Stores jobs in a MySQL table.
- Has an HTTP interface for scheduling and cancelling jobs.
- Makes a POST request to the endpoint defined in config on the job's execution time.

Install
-------

    $ go get github.com/cenkalti/dalga

Usage
-----

Configuration is done with a config file:

    $ dalga -config dalga.ini ...

Example config file is in the repository.

To create the table for storing jobs:

    $ dalga -config dalga.ini -create-table

Then, run the server:

    $ dalga -config dalga.ini

Schedule a new job to run every 60 seconds:

    $ curl -i -X PUT 'http://127.0.0.1:34006/jobs/check_feed/1234?interval=60'
    HTTP/1.1 201 Created
    Content-Type: application/json; charset=utf-8
    Date: Tue, 11 Nov 2014 22:10:40 GMT
    Content-Length: 89

    {"job":"1234","routing_key":"check_feed","interval":60,"next_run":"2014-11-11T22:11:40Z"}

60 seconds later, Dalga makes a POST to your endpoint defined in config:

    Path: <config.baseurl>/<job.path>
    Body: <job.body>

Endpoint must return 200 if the job is successful.
Anything other than 200 makes Dalga to retry the job indefinitely with an exponential backoff.

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
