FROM centos:7

# Install essential utils
RUN yum update -y && \
    yum install sudo hostname telnet net-tools vim tree less file java-1.8.0-openjdk-devel mysql -y && \
    yum clean all && rm -rf /var/cache/yum && rm -rf /var/tmp/yum-*

# Create user "admin" and add it into sudo group
RUN useradd -ms /bin/bash admin && \
    echo "admin:admin" | chpasswd && \
    echo "admin    ALL=(ALL)    NOPASSWD: ALL" >> /etc/sudoers


WORKDIR /home/admin

COPY polardbx-server/target/polardbx-server drds-server

# Remove localtime to make mount possible.
RUN rm -f /etc/localtime

# Copy entrypoint.sh
COPY docker/entrypoint.sh entrypoint.sh
RUN chmod 755 entrypoint.sh && \
    chown -R admin:admin entrypoint.sh && \
    chown -R admin:admin drds-server

USER admin

# Set command to entrypoint.sh
ENTRYPOINT /home/admin/entrypoint.sh
