

###Preparation###
######Selektion#######
 Auswahl der Datenquelle(n) mit Begründung.

###Ingest###
######Batch######
Import von relationalen Daten mit z.B. Sqoop.


######Streaming######
Kontinuierliche Datenaufnahme mit Flume oder Spark Streaming.

###Staging###
######Storage######
Auswahl und Erläuterung von:
- Dateiformate
- Komprimierungsformate


######Partitioning######
Erläuterung geeigneter Strategien für das Aufteilen der Daten in HDFS.


######Information Architecture######
Access Control Regeln und Organisation der Daten für Mandantenfähigkeit erläutern.


######Data Management######
Regeln für Retention/Lifecycle/Data Management erläutern.

###Processing###
######Transformation######
Daten mit Tools we Crunch oder Cascading aufbereiten.


######Analytics######
Daten mit Tools wie Spark oder Giraph analysieren.


######Machine Learning######
ML Modelle mit Spark oder MapReduce erstellen und anwenden.


###Access###
Zugang zu Daten demonstrieren, mit Hilfe von z.B. Kite SDK, JDBC über Hive oder Impala, der nativen APIs (HDFS, HBase).

###Automation###
Mit Oozie Data Pipelines automatisieren, mit Hilfe von Coordinators (also ereignis- oder zeitgesteuerte Verarbeitung).

###Production###
Pipeline mit Hilfe von Continuous Integration (CI) (z.B. Maven mit Jenkins) durchgängig testen. Beschreibung von Cluster Umgebungen.
