FROM debian:12 as build

RUN apt update -y && apt install -y wget unzip curl

# Install node.js 18 using nvm, then install yarn
RUN curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.1/install.sh | bash

ENV NVM_DIR /root/.nvm
ENV NODE_VERSION=18.20.6
RUN . $NVM_DIR/nvm.sh \
    && nvm install $NODE_VERSION \
    && nvm alias default $NODE_VERSION \
    && nvm use default

ENV NODE_PATH $NVM_DIR/v$NODE_VERSION/lib/node_modules
ENV PATH $NVM_DIR/versions/node/v$NODE_VERSION/bin:$PATH

RUN npm install --global yarn

# Download the source code for unitycatalog 0.2.1
RUN mkdir -p /build/unitycatalog
RUN wget -O /build/unitycatalog/unitycatalog-0.2.1.tar.gz https://github.com/unitycatalog/unitycatalog/archive/refs/tags/v0.2.1.tar.gz
RUN tar -xvf /build/unitycatalog/unitycatalog-0.2.1.tar.gz -C /build/unitycatalog

# Go to the /ui folder, and build the react app
RUN cd /build/unitycatalog/unitycatalog-0.2.1/ui \
    && yarn install \
    && yarn build


# Copy the compiled react app into an NGINX container to serve it
FROM nginx
COPY --from=build /build/unitycatalog/unitycatalog-0.2.1/ui/build/ /unitycatalog/html
COPY ./unitycatalog-ui.nginx.conf /etc/nginx/conf.d/default.conf