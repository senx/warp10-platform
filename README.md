[![Release](https://img.shields.io/github/v/release/senx/warp10-platform)](https://github.com/senx/warp10-platform/releases/latest)
[![Build Status](https://www.travis-ci.com/senx/warp10-platform.svg?branch=master)](https://www.travis-ci.com/senx/warp10-platform)
[![Tweet](https://img.shields.io/twitter/url/http/shields.io.svg?style=social)](https://twitter.com/intent/tweet?text=Get%20The%20Most%20Advanced%20Time%20Series%20Platform&url=https://warp10.io/download&via=warp10io&hashtags=tsdb,database,timeseries,opensource)

<p align="center"><a href="https://warp10.io" title="Warp 10 Platform"><img src="https://warp10.io/assets/img/warp10_bySenx_dark.png" alt="Warp 10 Logo" width="50%"></a></p>

# Warp 10
## Go further and faster in your journey to get value from your data

Warp 10 is a modular open source platform designed to collect, store and analyze sensors / time series data and any kind of sequence data in a horizontal and industrial perspective.

<p align="center"><a href="https://youtu.be/-5dAB7-dHaQ"><img src="https://warp10.io/assets/img/thumbnail_warp10_video.jpg" alt="Warp 10 simplifies sensor data management and analytics." width="50%"></a></p>

## Warp 10 simplifies data management and analytics
Shaped for the Internet of Things (IoT) with a flexible data model, Warp 10 provides a unique and powerful framework to simplify your processes from data collection to analysis and visualization, with the support of geolocated data in its core model (called Geo Time Series).

Geo Time Series extend the notion of Time Series by merging the sequence of sensor readings with the sequence of sensor locations. If your data have no location information, Warp 10 will handle them as regular Time Series.

Warp 10 offers both a Time Series Database and a powerful analysis environment that can be used together or independently.

## Features

The Warp 10 Platform provides a rich set of features to simplify your work on sensor data:
* **A powerful Analytics Engine** with [WarpLib](https://www.warp10.io/doc/reference), a library dedicated to time series data analysis with more than 1000 functions and extension capabilities to perform data analysis, from the simplest to the most advanced. Use the Analytics Engine integrated in the Warp 10 platform or as an external library in your tools.
* **Warp 10 Storage Engine**, our collection and storage layer, a Geo Time Series Database
* **The Edge version**, to implement Warp 10 on any machine technical system or device by adjunction of additional board or box thanks to a wide range of connectors.
* [**WarpScript**](https://www.warp10.io/content/03_Documentation/04_WarpScript), a language specifically designed for analytics of time series data. It is one of the pillars of the analytics layer of the Warp 10 Platform
* [**FLoWS**](https://www.warp10.io/content/03_Documentation/04_FLoWS), an alternative to WarpScript for users discovering the Warp 10 Platform. It is meant to be easy to learn, look familiar to users of other programming languages, and enable time series analysis by leveraging the whole of WarpLib.
* **Plasma and Mobius**, streaming engines allowing to cascade the Warp 10 Platform with Complex Event Processing solutions and to build dynamic dashboards
* **Runner**, a system for scheduling WarpScript program executions on the server side
* [**Sensision**](https://github.com/senx/sensision), a framework for exposing metrics and pushing them into Warp 10
* **Standalone version** running on a [Raspberry Pi](https://blog.senx.io/warp-10-raspberry-bench-for-industrial-iot/) as well as on a beefy server, with no external dependencies
* Replication and sharding of standalone instances using the **Datalog mechanism**
* **Distributed version**, based on Hadoop HBase for the most demanding environments
* Integration with [Pig](https://github.com/senx/warp10-pig), [Spark](https://github.com/senx/warp10-spark2), [Flink](https://github.com/senx/warp10-flink), [NiFi](https://github.com/senx/nifi-warp10-processor), [Kafka Streams](https://github.com/senx/warp10-plugin-kstreams) and [Storm](https://github.com/senx/warp10-storm) for batch and streaming analysis.
* An easy integration into a **large ecosystem** of existing tools, such as Jupyter, Python, HTTP, Json, NodeRed, R, Zeppelin, Tableau, Pytorch, MQTT, LevelDB, Avro and more.

A collection of tools that complete the Platform and ease your work on time series data:
* [WarpStudio](https://studio.senx.io/), a web editor, to edit and execute your WarpScript and FLoWS code.
* [WarpFleet](https://warpfleet.senx.io/), the artifact repository, to share your plugins, extensions and macros.
* [Sandbox](https://sandbox.senx.io), a hosted environment for test driving Warp 10 without deploying it.
* [WarpView](https://senx.github.io/warpview/), a collection of charting web components
* [Discovery](https://warp10.io/content/05_Ecosystem/02_Visualization/02_Discovery/00_Overview), a dynamic dashboarding solution with a unique dashboard as code approach.

## Getting started

We strongly recommend you to start with the [getting started](https://www.warp10.io/content/02_Getting_started).
You will learn the basics and the concepts behind Warp 10 step by step.

Learn more by browsing the [documentation](https://www.warp10.io/doc/reference).

To test Warp 10 without installing it, try the [free sandbox](https://sandbox.senx.io/) where you can get your hands on in no time.



## Help & Community

The team has put lots of efforts into the [documentation](https://www.warp10.io/doc/reference) of the Warp 10 Platform, there are still some areas which may need improving, so we count on you to raise the overall quality.

We understand that discovering all the features of the Warp 10 Platform at once can be intimidating, that’s why you have several options to find answers to your questions:
* Explore the [blog](https://blog.senx.io/) and especially the Tutorials and Thinking in WarpScript categories
* Explore the [tutorials](https://www.warp10.io/content/04_Tutorials) on [warp10.io](https://www.warp10.io/)
* Follow us on [Twitter](https://twitter.com/warp10io)
* Join the [Lounge](https://lounge.warp10.io/), the Warp 10 community on Slack
* Subscribe to the [Google Group](https://groups.google.com/forum/#!forum/warp10-users)
* Ask your question on StackOverflow using [warp10](https://stackoverflow.com/search?q=warp10) and [warpscript](https://stackoverflow.com/search?q=warpscript) tags
* Get informed of the last news of the Platform thanks to the [newsletter](https://senx.us19.list-manage.com/subscribe/post?u=322486758928f239c3698c600&id=bc195bb85c)

Our goal is to build a large community of users to move our platform into territories we haven't explored yet and to make Warp 10 and WarpScript the standards for sensor data and the IoT.

## Contributing to the Warp 10 Platform

Open source software is built by people like you, who spend their free time creating things the rest of the community can use.

You want to contribute to Warp 10? We encourage you to read the [contributing page](https://www.warp10.io/content/06_Community/02_Contributing) before.


## Commercial Support

Should you need commercial support for your projects, [SenX](https://senx.io/) offers support plans which will give you access to the core team developing the platform.

Don't hesitate to contact us at [sales@senx.io](mailto:sales@senx.io) for all your inquiries.

#### Trademarks

Warp 10, WarpScript, WarpFleet, Geo Time Series and SenX are trademarks of SenX S.A.S.
