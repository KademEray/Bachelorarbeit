FROM postgres:17.5

# ---------- Basis-Parameter ----------
ENV POSTGRES_DB=testdb \
    POSTGRES_USER=postgres \
    POSTGRES_PASSWORD=pass \
    LANG=en_US.utf8

# ---------- Custom-Tuning direkt in die Init-Vorlage mergen ----------
# Achtung: Pfad ist im offiziellen Image /usr/share/postgresql/postgresql.conf.sample
# (ohne Versions-Unterordner).  Falls Du ein Slim-/Alpine-Image nutzt, vorher verifizieren!
COPY postgres-tuning.conf /tmp/postgres-tuning.conf
RUN cat /tmp/postgres-tuning.conf >> /usr/share/postgresql/postgresql.conf.sample
