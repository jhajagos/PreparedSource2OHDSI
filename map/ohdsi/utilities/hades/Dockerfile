FROM rocker/tidyverse

# Install system dependencies
RUN apt-get update

RUN apt-get install -y openjdk-11-jdk && \
    apt-get install -y liblzma-dev && \
    apt-get install -y libbz2-dev

RUN apt-get install -y libpcre2-dev && \
    apt-get install -y libdeflate-dev && \
    apt-get install -y  libz-dev && \
    apt-get install -y  libxml2-dev && \
    apt-get install -y  build-essential && \
    apt-get install -y  libcurl4-openssl-dev && \
    apt-get install -y  libsodium-dev && \
    apt-get install -y  libfontconfig1-dev && \
    apt-get install -y  libharfbuzz-dev && \
    apt-get install -y  libfribidi-dev && \
    apt-get install -y  libfreetype6-dev libpng-dev libtiff5-dev libjpeg-dev

# Configure Java for R
RUN R CMD javareconf

# Install remotes and drat packages
RUN R -e "install.packages(c('remotes', 'drat'), repos='https://cloud.r-project.org/')"

# Add OHDSI drat repository and install HADES
RUN R -e "drat::addRepo('OHDSI'); install.packages('Hades', repos=c('https://cloud.r-project.org/', 'https://OHDSI.github.io/drat'))"

# Expose RStudio port
EXPOSE 8787

# Set working directory
WORKDIR /home/rstudio
