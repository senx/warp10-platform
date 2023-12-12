[![Release](https://img.shields.io/github/v/release/senx/warp10-platform)](https://github.com/senx/warp10-platform/releases/latest)
[![Build Status](https://app.travis-ci.com/senx/warp10-platform.svg?branch=master)](https://app.travis-ci.com/senx/warp10-platform)
[![Tweet](https://img.shields.io/twitter/url/http/shields.io.svg?style=social)](https://twitter.com/intent/tweet?text=Get%20The%20Most%20Advanced%20Time%20Series%20Platform&url=https://warp10.io/download&via=warp10io&hashtags=tsdb,database,timeseries,opensource)

<p align="center"><a href="https://warp10.io" title="Warp 10 Platform"><img src="https://warp10.io/assets/img/warp10_bySenx_dark.png" alt="Warp 10 Logo" width="50%"></a></p>

# Warp 10
## The Most Advanced Time Series Platform

Warp 10 is a modular open source platform shaped for the IoT that collects, stores and
allows you to analyze sensor data. It offers both a Time Series Database and
a powerful analysis environment that can be used together or independently.
[Learn more](https://www.warp10.io/content/01_About)

- **Increase the storage capacity** of your historical data and reduce your storage bill while preserving all analysis capabilities
- **Deploy a real time database** that scales with your time series needs
- **Enhance your existing tools** with a ubiquitous analysis environment dedicated to time series data
- **Streamlining KPIs and data** visualization across your organization
- **Enable your business applications** to interact easily with your system's data

## Improve the efficiency of your existing infrastructure

The Warp 10 Platform integrates into existing datalake infrastructures and provides storage and analytics solutions tailored for time series data which can be leveraged from existing tools.
![Reference Architecture](https://warp10.io/assets/img/archi/reference_architecture_warp10io-01.png)

| Component             | Description                                                                                                                                                                                                                                                                                                                                                                             |
|-----------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Storage Engine        | Securely ingest data coming from devices, supporting high throughput, delayed and out-of-order data with support for a wide variety of protocols such as HTTP, MQTT, or Kafka. [Read more](https://warp10.io/content/01_About/storage)                                                                                                                                                  |
| History Files         | Efficiently compact stable data and store the resulting files onto any filesystem or cloud object store while retaining the same access flexibility as data stored in the Warp 10 Storage Engine. [Read more](https://warp10.io/content/01_About/storage)                                                                                                                               |
| Analytics Engine      | Leverage **WarpLib**, a library of over 1300 functions designed specifically for time series data manipulation. Increase the efficiency of data teams thanks to the [WarpScript](https://www.warp10.io/content/03_Documentation/04_WarpScript) programming language, which uses WarpLib and interacts with a [large ecosystem](https://www.warp10.io/content/05_Ecosystem/00_Overview). |
| Dynamics Dashboards   | Create highly dynamic dashboards from your time series data. Discovery is a dashboard as code tool dedicated to Warp 10 technology. Display your data through an entire dashboard. [Read more](https://discovery.warp10.io/)                                                                                                                                                            |
| Business Applications | Enable business applications to benefit from the wealth of knowledge present in time series data by connecting those applications to the analytics and storage engines provided by the Warp 10 platform. [Read more](https://warp10.io/content/01_About/applications)                                                                                                                   |

>The Storage Engine, The Analytics Engine, History Files and Dynamics Dashboards can be used together or separately.
## Versions
The Warp 10 platform is available in three versions, **Standalone**, **Standalone+** and **Distributed**.
All versions provide the same level of functionality except for some minor differences, the complete **WarpScript** language is available in both versions. They differ mainly by the way the Storage Engine is implemented.

| Version     | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
|-------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Standalone  | The Standalone version is designed to be deployed on a single server whose size can range from a Raspberry Pi to a multi CPU box. It uses **LevelDB** as its storage layer or an in-memory datastore for cache setups. All features (storage, analysis) are provided by a single process, hence the name standalone. Multiple Standalone instances can be made to work together to provide High Availability to your deployment. This is provided via a replication mechanism called **Datalog**. |
| Standalone+ | Warp 10 with a **FoundationDB** backend. It is a middle ground between the standalone and distributed versions, basically a standalone version but with storage managed by FoundationDB instead of LevelDB.                                                                                                                                                                                                                                                                                       |
| Distributed | The Distributed version coordinates multiple processes on multiple servers. The Storage layer uses **FoundationDB** for data persistence. Communication between processes is done through Kafka and ZooKeeper. This version is suitable for heavy workloads and giant datasets. Scalability comes with a price, the added complexity of the architecture.                                                                                                                                         |

## Getting started

We strongly recommend that you start with the [Onboarding tutorials](https://studio.senx.io/) to learn how Warp 10 works, and how to perform basic operations with WarpScript.
To deploy your own instance, read the [getting started](https://www.warp10.io/content/02_Getting_started).

Learn more by browsing the [documentation](https://www.warp10.io/doc/reference).

To test Warp 10 without installing it, try the [free sandbox](https://sandbox.senx.io/) where you can get your hands on in no time.

For quick start:
```bash
./warp10.sh init standalone
./warp10.sh start
```


## Help & Community

The team has put lots of efforts into the [documentation](https://www.warp10.io/doc/reference) of the Warp 10 Platform,
there are still some areas which may need improving, so we count on you to raise the overall quality.

We understand that discovering all the features of the Warp 10 Platform at once can be intimidating, that’s why you have several options to find answers to your questions:
* Explore the [blog](https://blog.senx.io/) and especially the Tutorials and Thinking in WarpScript categories
* Explore the [tutorials](https://www.warp10.io/content/04_Tutorials) on [warp10.io](https://www.warp10.io/)
* Join the [Lounge](https://lounge.warp10.io/), the Warp 10 community on Slack
* Ask your question on StackOverflow using [warp10](https://stackoverflow.com/search?q=warp10) and [warpscript](https://stackoverflow.com/search?q=warpscript) tags
* Get informed of the last news of the platform thanks to [Twitter](https://twitter.com/warp10io) and the [newsletter](https://senx.us19.list-manage.com/subscribe/post?u=322486758928f239c3698c600&id=bc195bb85c)

Our goal is to build a large community of users to move our platform into territories we haven't explored yet and to make Warp 10 and WarpScript the standards for sensor data and the IoT.

## Contributing to the Warp 10 Platform

Open source software is built by people like you, who spend their free time creating things the rest of the community can use.

You want to contribute to Warp 10? We encourage you to read the [contributing page](https://www.warp10.io/content/06_Community/02_Contributing) before.


## Commercial Support

Should you need commercial support for your projects, [SenX](https://senx.io/) offers support plans which will give you access to the core team developing the platform.

Don't hesitate to contact us at [sales@senx.io](mailto:sales@senx.io) for all your inquiries.

#### Trademarks

Warp 10, WarpScript, WarpFleet, Geo Time Series and SenX are trademarks of SenX S.A.S.
