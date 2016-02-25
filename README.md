# Big Data Engineering - FH Münster - Projekt

[![Build Status](https://travis-ci.com/kutzilla/bde-project.svg?token=sVFsn6MbRsFLvenMx9sG&branch=master)](https://travis-ci.com/kutzilla/bde-project)


Alle Informationen zu dem Bewertungsschema sind der in der Präsentation.
Dazu hier weitere Ergänzungen:

###Staging###
####Storage#####
Auswahl und Erläuterung von:
- Dateiformate
	- alle Dateien liegen im Json vor und werden strukturiert von den APIs (Github, Google Geo) zurückgeliefert und zur weiteren Verarbeitung beibehalten.
- Komprimierungsformate
	- zur Archivierung der verarbeiteten User und Commit Daten (Pig-Scripte, Verschieben vom Ordner Processing nach Archive) werden die Dateien im B2Zip Format komprimiert und verschoben.


####Data Management####
Regeln für Retention/Lifecycle/Data Management erläutern.
- die Verarbeiteten Daten werden, wie erwähnt bereits archiviert im HDFS
- die resultierenden Nutzdaten im HBase für die Analyse im Frondend können nach einem bestimmten Zeitraum (Speichermangel oder zu alt) archiviert und aus der HBase entfernt werden.

