<img width="600" alt="sk" src="https://user-images.githubusercontent.com/1049678/189514933-9182db0a-f746-4b7b-9840-10715e5f9199.png">
<img src="https://user-images.githubusercontent.com/1049678/189514907-f953af27-6136-496b-b9f4-4dd5110deff7.png">

# signalk-to-influxdb2

This is a [Signal K Server](https://github.com/SignalK/signalk-server) plugin that writes Signal K data into one or several [InfluxDB v2](https://docs.influxdata.com/influxdb/v2.4/) databases.

This is a total rewrite of [signalk-to-influxdb](https://www.npmjs.com/package/signalk-to-influxdb) that works with InfluxDB v1.x databases.

Once the data is in InfluxDb you can use for example [Grafana](http://grafana.org/) to draw pretty graphs of your data.

Main features:
- writes `navigation.position` shaped to work with InfluxDB's [geo features](https://docs.influxdata.com/influxdb/v2.1/query-data/flux/geo/)
- supports multiple InfluxDb connections

See [backlog](https://github.com/users/tkurki/projects/1/views/1) for planned features and [Releases](https://github.com/tkurki/signalk-to-influxdb2/releases) for published features.

For discussions and support visit the [Signal K Website](https://signalk.org/).

### Details

- data for Signal K "self" vessel is tagged with tag `self` with value `t`

# Plugin and InfluxDB Setup

To configure the plugin, you need to have have already installed InfluxDB 2 and created a new bucket and an access token.
You can use the operator API token or, if you're security-conscious, create a new token with more restrictive permissions.

Install the plugin using the Signal K Appstore. Remember to restart Signal K to apply the changes.

Go to plugin configuration: Server -> Plugin Config, then scroll down to "Signal K to InfluxDb 2" and expand the section.

The plugin can connect to multiple InfluxDB instances. We will only configure one:

- Add a new Influx instance by clicking the plus sign
- Url: http://127.0.0.1:8086
- Set the token to the one you created earlier
- Organization: (same as in the InfluxDB config)
- Bucket: (same as in the InfluxDB config)

If you're mostly interested in your own boat data, select "Store only self data".
Leaving that unchecked may result in a lot of data being stored, especially if you are in a busy area.

Set the other configuration options as to your liking. Brief guidance:

- Ignored paths and sources are important for limiting the amount of data. However, don't set them until you have verified data is being stored in the DB.
- "Use timestamps from SK data": Usually you want to use the original timestamps, but rewriting the timestamps may result in more efficient DB operations.
- "Resolution": typically the maximum data frequency on boat networks is 10 Hz. If you set a value of 100 ms or less, you will capture all data, at the cost of a lot of used disk space. A value of 1000 ms is reasonable for real-time observations, and 5000 ms or even 10&nbsp;000 ms for history logging purposes.
- "Flush interval" dictates how often data is written to the disk. Use the same value as in "Resolution" as the initial setting.
- Remaining settings can be safely skipped.

Remember to click "Submit" to save your configuration!

## Configuring the Grafana Data Source

While technically not in the scope of this plugin, this section describes how to set up an InfluxDB data source in Grafana. (Another option would be to use the experimental [signalk-grafana data](https://github.com/tkurki/signalk-grafana) data source.) These settings have been tested with InfluxDB 2.7.10, earlier versions may or may not work.

These instuctions configure an InfluxDb datasource using InfluxQL as the query language against InfluxDb version 2. InfluxQL is a good choice as the query language because the Grafana datasource includes a UI for building queries by picking database fields instead of manually writing the raw query.

To begin with you will need to [create an API token in InfluxDB](https://docs.influxdata.com/influxdb/v2/admin/tokens/create-token/).

Login to Grafana. On Signal K installations, Grafana often runs on port 3001: http://mysignalkhostname:3001/

Create the data source:

- Click on "Add your first data source"
  - Data source type: Influxdb
  - Name: influxdb
  - Query language: InfluxQL
  - HTTP URL: http://localhost:8086

Use the following InfluxDB Details:

- Database: `your bucket name`
- User: `your influxdb user`
- Password: `your API Token`
- HTTP Method: either `GET` or `POST`

Then click "Save & test".
You should get a positive acknowledgment. Now, you can create dashboard panels that use InfluxDB as the data source.

# History API

The plugin implements the Signal K History API, allowing retrieval of historical data with HTTP GET requests.

## Retrieve logged data

To retrieve logged data from the database, send a request to the `values` endpoint, specifying at least the time interval and SignalK path(s).

`http://localhost:3000/signalk/v1/history/values?from=2023-04-23T18:53:20Z&to=2023-04-23T18:55:00Z&paths=environment.wind.speedApparent`

The data returned is in SI units, e.g. vessel and wind speeds in meters/s, angles in radian.

For further refinement of the queries, you can use the following request parameters.

### Request Parameters

The following request parameters are supported in queries:

* `from` / `to` (mandatory)

  Start and end timestamp, GMT based, e.g. 2023-11-12T21:34:00Z 

* `paths` (mandatory)

  SignalK paths to retrieve, comma-separated.
  By default the mean value is returned, but a path can be appended with :min or :max to select the minimum or maximum value.

* `resolution` (optional)

  The time interval in seconds between each point returned. For example `http://localhost:3000/signalk/v1/history/values?paths=navigation.position&from=2023-11-06T17:30:47Z&to=2023-11-12T21:34:00Z&resolution=60` will return the data with one position per minute. The data is simply sampled with InfluxDB's `first()` function.

* `format` (optional)

  Position data ('navigation.position') can be exported as JSON (default) or in [GPX](https://www.topografix.com/gpx.asp) format. Set `format=gpx` to receive a GPX track.

### Examples

Get minimum apparent wind angles:

`http://localhost:3000/signalk/v1/history/values?from=2023-04-23T18:51:26Z&to=2023-04-23T19:58:43Z&paths=environment.wind.angleApparent:min`

Get wind angle and boat speed:

`http://localhost:3000/signalk/v1/history/values?from=2023-04-23T18:53:20Z&to=2023-04-23T18:55:00Z&paths=environment.wind.angleApparent,navigation.speedThroughWater&resolution=60`

Get the track with positions at intervals of 5 minutes in GPX format:

`http://localhost:3000/signalk/v1/history/values?from=2023-11-04T10:00:00Z&to=2023-11-12T18:00:00Z&paths=navigation.position&resolution=300`

## List available data paths

To retrieve a list of available paths, send a request to the `paths` endpoint with from and to timestamps:

`http://localhost:3000/signalk/v1/history/paths?from=2023-11-04T00:00:00Z&to=2029-11-25T23:59:59Z`


## List available contexts

To retrieve a list of available contexts, send a request to the `contexts` endpoint with from and to timestamps:

`http://localhost:3000/signalk/v1/history/contexts?from=2023-11-04T00:00:00Z&to=2029-11-25T23:59:59Z`
