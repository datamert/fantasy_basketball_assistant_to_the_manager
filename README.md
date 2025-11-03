# Fantasy Basketball Assistant (to the) Manager
## 1. Background
The Fantasy Basketball Assistant (to the) Manager is a full-stack data (minimum viable) product designed to empower fantasy basketball managers worldwide with advanced gamelog statistics and real-time updates from NBA news and injury reports. Fantasy basketball is a game where players act as general managers of virtual basketball teams that consist of NBA players and and their success is tied to the real-life performance of these players, based on varying scoring settings. Fantasy basketball managers are often limited to the analytics provided by their game platform i.e. data that is available to all competitors and therefore offers no strategic edge. They need an assistant manager (or an assistant to the manager — shoutout to Dwight Schrute from _The Office_) that can provide them intelligence through data to guide decision making, whether it's drafting players at the start of the season or trading them during the season. This is an attempt to fill in this very gap.

## 2. Architecture Overview
The Fantasy Basketball Assistant (to the) Manager is built primarily on Microsoft Fabric, using data from NBA and ESPN, with Google Cloud Platform acting as an intermediary.

![Data ingestion view](readme_images/data_ingestion_view.png)

The most important dataset is player gamelogs which are queried from NBA using the open-source nba_api.py package. The challenge with using this package in a Fabric environment is that the IPs used by Fabric Notebooks—like most cloud platform IPs—are blocked by the NBA API and therefore calls to dynamic endpoints return a ReadTimeout error: HTTPSConnectionPool(host='stats.nba.com', port=443): Read timed out. (read timeout=30). Research on this topic with help from Copilot led me to use Google Cloud Platform that had whitelisted IPs. So I built a Google Job (run with Google Scheduler) that is a Docker image based on a Python script that queries the playergamelogs endpoint. To avoid the overhead of storing the query result in an intermediary storage and then having to run an ETL from Fabric, I opted to use the Open Mirroring Database feature on Fabric. This way, I am able to write the query result directly into a OneLake Landing Zone that are then automatically converted to managed delta tables in Fabric. Building on the Google Job for gamelogs, I also created Google Jobs to query data from ESPN and NBA injury reports, using parserfeed and nbainjuries.py respectively, and write them directly to a Custom Endpoint of Fabric Evenstream. These jobs can be run at a high frequency to simulate real-time data.


![Fabric lineage view](readme_images/fabric_lineage_view.png)

After the data lands to the Fabric workspace, there are multiple components (described in detail in the next section) that power:
- **Latest NBA News and Injuries:** a Real-Time Dashboard that visualizes, in real-time, the injury status of players by game along with the latest ESPN news on the NBA. This helps fantasy basketball managers to decide their starting roster and stream 'injury-replacement' players by providing them the latest news in one place.
- **Injury Tracker of My Players:** an Activator with an alert that sends an email when one of the manager's players' injury status changes. This alert fantasy basketball managers to make roster changes in time for upcoming games.
- **NBA Player Gamelog Analysis:** a Power BI report that provides insights into each NBA player's stats and fantasy points with dynamic scoring settings. This provides an in-depth dive to each player's profile as well as a player ranking list that can be based on statistical metrics different to the default rankings of fantasy basketball platforms.

## 3. Components
### 3.1. Google Jobs
#### 3.1.1. generate_gamelog
Text
#### 3.1.2. generate_injury_report
Text
#### 3.1.3. generate_nba_news
Text
### 3.2. Fabric
#### 3.2.1. Open Mirroring
##### 3.2.1.1. Create Materialized Lake Views.Notebook
Text
##### 3.2.1.2. NBA API Google Job Mirror.MirroredDatabase
Text

## 4. Setup & Usage
Text

## 5. License
Text




