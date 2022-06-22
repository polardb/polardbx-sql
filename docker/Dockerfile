FROM polardbx/galaxybasejava:v20220621

WORKDIR /home/admin

COPY polardbx-server/target/polardbx-server drds-server
COPY docker/admin/bin/* /home/admin/bin/
COPY docker/etc/* /tmp/
COPY docker/entrypoint.sh entrypoint.sh

RUN \
    chmod a+x bin/* && \
    chmod a+x entrypoint.sh && \
    chown admin:admin -R /home/admin && \
    cp /tmp/drds /etc/logrotate.d/drds && \
    cp /tmp/log_cleaner /etc/cron.d/log_cleaner && \
    true

USER admin

# Set command to entrypoint.sh
ENTRYPOINT /home/admin/entrypoint.sh