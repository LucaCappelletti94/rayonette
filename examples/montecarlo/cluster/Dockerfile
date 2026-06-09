# A blank worker for the Monte Carlo swarm: an ssh server with NO rust. rayonette
# provisions it from cold (install rust, ship the source, build the agent,
# launch it). The only things baked in are sshd and the tools rustup and the
# compiler need.
FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openssh-server curl ca-certificates gcc libc6-dev \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /run/sshd /root/.ssh \
    && chmod 700 /root/.ssh \
    && ssh-keygen -A \
    && printf 'PermitRootLogin prohibit-password\nPasswordAuthentication no\n' \
        > /etc/ssh/sshd_config.d/montecarlo.conf
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
