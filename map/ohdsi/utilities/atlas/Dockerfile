FROM ubuntu:22.04

# Set environment variables
ENV TOMCAT_VERSION=9.0.110 \
    CATALINA_HOME=/opt/tomcat \
    JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64/

# Install dependencies
RUN apt-get update && apt-get install -y \
    nginx \
    openjdk-8-jdk \
    wget \
    curl \
    supervisor \
    vim \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Download and install Tomcat
RUN wget https://archive.apache.org/dist/tomcat/tomcat-9/v${TOMCAT_VERSION}/bin/apache-tomcat-${TOMCAT_VERSION}.tar.gz \
    && tar xzf apache-tomcat-${TOMCAT_VERSION}.tar.gz \
    && mv apache-tomcat-${TOMCAT_VERSION} ${CATALINA_HOME} \
    && rm apache-tomcat-${TOMCAT_VERSION}.tar.gz \
    && chmod +x ${CATALINA_HOME}/bin/*.sh

# Configure Nginx as reverse proxy
RUN rm /etc/nginx/sites-enabled/default

COPY <<EOF /etc/nginx/sites-available/tomcat-proxy
server {
    listen 80;
    server_name localhost;

    # Access and error logs
    access_log /var/log/nginx/tomcat-access.log;
    error_log /var/log/nginx/tomcat-error.log;

    # Proxy settings
    location / {
        proxy_pass http://localhost:8080;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;

        # WebSocket support
        proxy_http_version 1.1;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection "upgrade";
    }

    # Serve static files directly (optional)
    location ~* \.(jpg|jpeg|png|gif|ico|css|js)$ {
        proxy_pass http://localhost:8080;
        expires 1y;
        add_header Cache-Control "public, immutable";
    }
}
EOF

RUN ln -s /etc/nginx/sites-available/tomcat-proxy /etc/nginx/sites-enabled/

# Configure Supervisor to manage both services
COPY <<EOF /etc/supervisor/conf.d/services.conf
[supervisord]
nodaemon=true

[program:tomcat]
command=${CATALINA_HOME}/bin/catalina.sh run
autostart=true
autorestart=true
stderr_logfile=/var/log/tomcat.err.log
stdout_logfile=/var/log/tomcat.out.log

[program:nginx]
command=/usr/sbin/nginx -g "daemon off;"
autostart=true
autorestart=true
stderr_logfile=/var/log/nginx.err.log
stdout_logfile=/var/log/nginx.out.log
EOF

# Create webapps directory for WAR deployment
RUN mkdir -p ${CATALINA_HOME}/webapps

RUN cd ${CATALINA_HOME}/webapps && wget https://github.com/OHDSI/WebAPI/releases/download/v2.15.1/WebAPI.war && mkdir ./WebAPI/ && cd ./WebAPI/ && unzip ../WebAPI.war

RUN cd ${CATALINA_HOME}/webapps && wget https://github.com/OHDSI/Atlas/releases/download/v2.15.0/atlas.zip && unzip atlas.zip


# Expose Nginx port
EXPOSE 80
EXPOSE 8080

# Health check
#HEALTHCHECK --interval=30s --timeout=3s --start-period=40s --retries=3 \
#    CMD curl -f http://localhost/ || exit 1

# Start supervisor
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/supervisord.conf"]