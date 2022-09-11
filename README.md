<img width="600" alt="sk" src="https://user-images.githubusercontent.com/1049678/189514933-9182db0a-f746-4b7b-9840-10715e5f9199.png">
<img src="https://user-images.githubusercontent.com/1049678/189514907-f953af27-6136-496b-b9f4-4dd5110deff7.png">

# signalk-to-influxdb2

This is a [Signal K Server](https://github.com/SignalK/signalk-server) plugin that writes Signal K data into one or several [InfluxDB v2](https://docs.influxdata.com/influxdb/v2.4/) databases.

This is a total rewrite of [signalk-to-influxdb](https://www.npmjs.com/package/signalk-to-influxdb) that works with InfluxDB v1.x databases.

Main features:
- writes `navigation.position` shaped to work with InfluxDB's [geo features](https://docs.influxdata.com/influxdb/v2.1/query-data/flux/geo/)
- supports multiple InfluxDb connections

See [backlog](https://github.com/users/tkurki/projects/1/views/1) for planned features and [Releases](https://github.com/tkurki/signalk-to-influxdb2/releases/tag/v0.0.6) for published features.

Discussion in [Signal K Slack](https://signalk-dev.slack.com/) ([get invited](http://slack-invite.signalk.org/)).
