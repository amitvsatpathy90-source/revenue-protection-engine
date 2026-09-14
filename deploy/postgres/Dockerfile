# Keep PostgreSQL patch version pinned for reproducible image builds.
FROM postgres:16.3

# postgresql-16, postgresql-client-16, libpq5 are already 16.3-1.pgdg120+1 in the base
# image — nothing to install there. postgresql-16-pgvector Breaks: postgresql-16-jit-llvm
# (< 19), and this postgresql-16 build Provides jit-llvm=16 as part of itself — APT's
# solver refuses the transaction outright since it can't remove that virtual provide
# without removing postgresql-16. Real ABI-version conflict, not a mispin; RPE never
# invokes JIT (see jit=off in compose), so forcing past it is safe.
RUN apt-get update \
    && apt-get download postgresql-16-pgvector=0.8.6-1.pgdg12+1 \
    && dpkg --force-breaks --force-depends -i postgresql-16-pgvector_*.deb \
    && rm -f postgresql-16-pgvector_*.deb \
    && rm -rf /var/lib/apt/lists/*