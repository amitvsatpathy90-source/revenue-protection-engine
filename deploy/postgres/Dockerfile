# Pinned patch version so image builds stay reproducible
FROM postgres:16.3

RUN apt-get update \
    # Grab the exact pgvector .deb instead of letting apt resolve it
    && apt-get download postgresql-16-pgvector=0.8.6-1.pgdg12+2 \
    # Force flags skip dependency checks; only safe for this verified combo
    && dpkg --force-breaks --force-depends -i postgresql-16-pgvector_*.deb \
    # Cleanup in the same layer so the image stays small
    && rm -f postgresql-16-pgvector_*.deb \
    && rm -rf /var/lib/apt/lists/*