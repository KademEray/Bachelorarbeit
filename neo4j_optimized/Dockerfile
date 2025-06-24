FROM neo4j:5.26.6

# 1. Neo4j-Basis-Login & evtl. weiteres ENV
ENV NEO4J_AUTH=neo4j/superpassword55

# 2. Konfig kopieren
COPY --chown=neo4j:neo4j neo4j.conf /var/lib/neo4j/conf/neo4j.conf
