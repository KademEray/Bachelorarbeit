# Verwendet das offizielle Neo4j-Image in Version 5.26.6 als Basis
FROM neo4j:5.26.6

# 1. Setzt die Umgebungsvariable für die Authentifizierung (Benutzer:Passwort)
ENV NEO4J_AUTH=neo4j/superpassword55

# 2. Kopiert eine benutzerdefinierte Konfigurationsdatei in den vorgesehenen Pfad innerhalb des Containers
COPY --chown=neo4j:neo4j neo4j.conf /var/lib/neo4j/conf/neo4j.conf
