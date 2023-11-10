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

Discussion in [Signal K Slack](https://signalk-dev.slack.com/) ([get invited](http://slack-invite.signalk.org/)).

### Details

- data for Signal K "self" vessel is tagged with tag `self` with value `t`

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

  SignalK paths to retrieve, comma-separated. Note that `navigation.position` can not be combined with other paths.
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
