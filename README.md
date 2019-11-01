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

    $ go get github.com/cenkalti/dalga/cmd/dalga

Usage
-----

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
    Content-Length: 83

    {"path":"check_feed","body":"1234","interval":60,"next_run":"2014-11-11T22:11:40Z"}

PUT always returns 201. If there is an existing job with path and body, it will be rescheduled.

There are 4 options that you can pass to `Schedule` but not every combination is valid:

| Param | Description | Type | Example |
| ----- | ----------- | ---- | ------- |
| interval | Run job at intervals in seconds | Integer | 60 |
| first-run | Do not run job until this time | RFC3339 Timestamp | 1985-04-12T23:20:50.52Z |
| one-off | Run job only once | Boolean | true, false, yes, no, on, off, 1, 0 |
| immediate | Run job immediately as it is scheduled | Boolean | true, false, yes, no, on, off, 1, 0 |

60 seconds later, Dalga makes a POST to your endpoint defined in config:

    Path: <config.baseurl>/<job.path>
    Body: <job.body>

The endpoint must return 200 if the job is successful.

The endpoint may return 204 if job is invalid. In this case Dalga will remove the job from the table.

Anything other than 200 or 204 makes Dalga to retry the job indefinitely with an exponential backoff.

Get the status of a job:

    $ curl -i -X GET 'http://127.0.0.1:34006/jobs/check_feed/1234'
    HTTP/1.1 200 OK
    Content-Type: application/json; charset=utf-8
    Date: Tue, 11 Nov 2014 22:12:21 GMT
    Content-Length: 83

    {"path":"check_feed","body":"1234","interval":60,"next_run":"2014-11-11T22:12:41Z"}

GET may return 404 if job is not found.

Cancel previously scheduled job:

    $ curl -i -X DELETE 'http://127.0.0.1:34006/jobs/check_feed/1234'
    HTTP/1.1 204 No Content
    Date: Tue, 11 Nov 2014 22:13:35 GMT

You may trigger a job manually by sending a POST request:

    $ curl -i -X POST 'http://127.0.0.1:34006/jobs/check_feed/1234?interval=60'
    HTTP/1.1 200 OK
    Content-Type: application/json
    Date: Fri, 14 Nov 2014 09:09:01 GMT
    Content-Length: 83

    {"path":"check_feed","body":"1234","interval":0,"next_run":"2014-11-14T09:09:01Z"}
