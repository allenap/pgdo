FROM ubuntu:latest

SHELL ["/bin/bash", "-c"]

# Like `ENV`, but not persisted in the final image.
ARG DEBIAN_FRONTEND=noninteractive

# Install build tools and other prerequisites.
RUN apt-get update && apt-get install -y build-essential curl gnupg sudo

# Set up PostgreSQL APT repository, https://wiki.postgresql.org/wiki/Apt.
RUN apt-get install -y postgresql-common
RUN sh /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh -y
RUN apt-get update

# Install various PostgreSQL versions.
#
# See https://www.postgresql.org/support/versioning/ for official support
# information, and https://apt.postgresql.org/pub/repos/apt/ for all available
# versions. At present only version 11 and above are officially supported
# upstream (and version 11 will get its final release in November 2023 and
# officially enter EOL), but there is still logic in pgdo that works with
# versions as old as 9.0.
#
RUN apt-get install -y postgresql-{9.{0,1,2,3,4,5,6},10,11,12,13,14,15,16,17}

# Remove any preexisting `ubuntu` user and groups.
RUN if getent passwd ubuntu >/dev/null 2>&1; then userdel --remove ubuntu; fi
RUN if getent group ubuntu >/dev/null 2>&1; then groupdel ubuntu; fi

# Add a non-root user.
RUN groupadd pgdo --gid 1000
RUN \
  useradd pgdo --uid 1000 --gid pgdo --groups sudo \
  --home-dir /home/pgdo --create-home --shell /bin/bash
RUN echo "pgdo ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/pgdo

# From now on, run as `pgdo`.
WORKDIR /home/pgdo
USER pgdo

# Install Rust; add Cargo to `PATH`.
ADD --chown=pgdo:pgdo --chmod=755 https://sh.rustup.rs /tmp/rustup-init
RUN /tmp/rustup-init -y --component clippy && rm /tmp/rustup-init
ENV PATH="/home/pgdo/.cargo/bin:${PATH}"

# Install SQLx tooling.
RUN cargo install sqlx-cli --no-default-features --features postgres

# Add the most recent PostgreSQL runtime to `PATH`. Some tests in `pgdo` scan
# `PATH` for runtimes; this will allow them to pass. It's also convenient.
ENV PATH="/usr/lib/postgresql/16/bin:${PATH}"

# Ensure that `USER` is set. At present this is required by `pgdo`, but
# https://github.com/allenap/pgdo/issues/28 may resolve that.
ENV USER="pgdo"

# End.
