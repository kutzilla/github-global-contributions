# Kommandos

Ausgangslage für die folgende Befehlt ist die virtuelle Maschine von Cloudera in Version 5.5.
Der Download Link ist http://www.cloudera.com/downloads/cdh/5-5-2.html.

### 1. Installation und Konfiguration

In diesem Teil wird die allgemeine Installation des Projekt beschrieben.

#### 1.1 Installation der Artefakte

Klonen des Repositories:

`git clone git@github.com:kutzilla/bde-project.git`

Wechseln in das Verzeichnis:

`cd bde-project`

Bauen der Artefakte:

`mvn clean package`

#### 1.2 Konfiguration von Flume

Wechseln in das Konfigurationsverzeichnis von Flume:

`cd /etc/alternatives/flume-ng-conf`

Öffnen der Konfigurationen:

`sudo nano flume.conf`

Einfügen der folgenden Konfigurationen:

```#Benennung der Komponenten des Agents
GithubAgent.sources = remote-client
GithubAgent.sinks = hdfs-sink
GithubAgent.channels = mem-channel-commit-events

#Beschreibung/Konfiguration der Quelle
GithubAgent.sources.remote-client.type = avro
GithubAgent.sources.remote-client.channels = mem-channel-commit-events
GithubAgent.sources.remote-client.bind = 127.0.0.1
GithubAgent.sources.remote-client.port = 41414
GithubAgent.sources.remote-client.selector.type = multiplexing
GithubAgent.sources.remote-client.selector.header = EventType
GithubAgent.sources.remote-client.selector.mapping.CommitEvent = mem-channel-commit-events
GithubAgent.sources.remote-client.selector.default = mem-channel-commit-events

#Beschreibung/Konfiguration des Abflusses
GithubAgent.sinks.hdfs-sink.type = hdfs
GithubAgent.sinks.hdfs-sink.channel = mem-channel-commit-events
GithubAgent.sinks.hdfs-sink.hdfs.useLocalTimeStamp = true
GithubAgent.sinks.hdfs-sink.hdfs.path = hdfs://localhost:8020/user/cloudera/data/repos/raw/%{OwnerName}/%{RepositoryName}/%{EventType}/%{Committer}
GithubAgent.sinks.hdfs-sink.hdfs.filePrefix = commits-
GithubAgent.sinks.hdfs-sink.hdfs.fileSuffix = .dat
GithubAgent.sinks.hdfs-sink.hdfs.batchSize = 100
GithubAgent.sinks.hdfs-sink.hdfs.round = true
GithubAgent.sinks.hdfs-sink.hdfs.roundValue = 10
GithubAgent.sinks.hdfs-sink.hdfs.roundUnit = minute
GithubAgent.sinks.hdfs-sink.hdfs.writeFormat = Text
GithubAgent.sinks.hdfs-sink.hdfs.fileType = DataStream
GithubAgent.sinks.hdfs-sink.hdfs.rollInterval = 30
GithubAgent.sinks.hdfs-sink.hdfs.rollCount = 30



#Beschreibung/Konfiguration des Kanals
GithubAgent.channels.mem-channel-commit-events.type = memory
GithubAgent.channels.mem-channel-commit-events.capacity = 10000
GithubAgent.channels.mem-channel-commit-events.transactionCapacity = 200
```

Bearbeiten der Flume Enviorments:

`sudo nano flume-env.sh`

Enivorment mit dem folgenden Inhalten füllen:

```
export JAVA_HOME=/usr/java/jdk1.7.0_67-cloudera/
```

Wechseln in das Flume bin-Verzeichnis und Flume starten:

`cd /usr/lib/flume-ng/bin`

`./flume-ng agent -n GithubAgent -c conf -f /etc/flume-ng/conf/flume.conf Dflume.root.logger=DEBUG,console -n GithubAgent
`

### 2. Durchführung

Manuelle Durchführung der Java Jobs:

#### 2.1 Abfragen der Github Commit Daten

Ausführen der Github Commits:

`cd ~/bde-project/github-commit-ingest/target`

`hadoop jar github-commit-data.jar`

#### 2.2 User Daten stagen

Ausführen des Pig Skripts:

```
fs -touchz data/users/raw/user-tmp;
fs -mv data/users/raw/user-* data/users/processing/;
fs -rm data/users/processing/user-tmp;``
```

#### 2.3 Abfragen der Github User Daten

Ausführen der Github User:

`cd ~/bde-project/github-user-ingest/target`

`hadoop jar github-user-ingest.jar`

#### 2.4 User mit Pig archivieren

Folgendes Pig Script ausführen:

```
%default TODAYS_DATE `date +%Y%m%d%H%M`;

--comma seperated list of hdfs directories to compress
input0 = LOAD 'data/users/processing/*' USING PigStorage();

-- compress and store into directory --
STORE input0 INTO 'data/users/archive/$TODAYS_DATE' USING PigStorage();

-- remove directory --
fs -rm data/users/processing/*
```

#### 2.5 Geolocation Daten einlesen

Folgende Jar ausführen:

`cd ~/bde-project/geo-data-ingest/target`

`hadoop jar geo-data-ingest.jar`

#### 2.6 Commits stagen

Folgendes Pig Skript ausführen:

````
fs -touchz data/repos/raw/tmp/tmp/tmp/tmp/tmp.dat;
fs -mv data/repos/raw/*/*/*/*/*.dat data/repos/processing/;
fs -mv data/repos/processing/tmp.dat data/repos/processing/tmp.placeholder;
````

### 2.7 Ausführen vom Processing

Das Processing mit folgenden Befehlen ausführen:

`cd ~/bde-project/github-data-processing/target`

`spark-submit github-data-processing.jar`

### 2.8 Komprierung und Archivieren von Commits

Ausführen des folgenden Pig Skripts:

```
%default TODAYS_DATE `date +%Y%m%d%H%M`;

--comma seperated list of hdfs directories to compress
input0 = LOAD 'data/repos/processing/*' USING PigStorage();

-- compress and store into directory --
STORE input0 INTO 'data/repos/archive/$TODAYS_DATE' USING PigStorage();

-- remove directory --
fs -rm data/repos/processing/*
```


## 3. Deployment

Im Folgenden wird das Deployment beschrieben.

### 3.1 Download Apache Tomcat 8 und entpacken
http://apache.mirror.iphh.net/tomcat/tomcat-8/v8.0.32/bin/apache-tomcat-8.0.32.tar.gz

Kopieren nach /opt/apache-tomcat-8.0.32

####Apache Port auf 8090 ändern:
    Bearbeiten der Konfigurationsdatei: /opt/apache-tomcat-8.0.32/conf/server.xml
    ```
    ...
    <Connector port="8090" protocol="HTTP/1.1"
                   connectionTimeout="20000"
                   redirectPort="8443" />
        <!-- A "Connector" using the shared thread pool-->
        <!--
        <Connector executor="tomcatThreadPool"
                   port="8090" protocol="HTTP/1.1"
                   connectionTimeout="20000"
                   redirectPort="8443" />
        -->
    ...
    ```

Kopieren    github-data-RestHbaseInterface/target/global-github-contributions.war   nach    /opt/apache-tomcat-8.0.32/webapps/
Kopieren    github-global-presentation/target/github-global-presentation.war    nach    /opt/apache-tomcat-8.0.32/webapps/

#### Starten des Tomcats
sudo bash /bin/catalina.sh start


####Webseite erreichbar unter:
http://localhost:8090/global-github-contributions/rest/json/github/getAllCommitsOfAllCountries
