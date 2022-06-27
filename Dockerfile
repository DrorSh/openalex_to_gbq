FROM node

RUN npm install -g chalk
RUN npm install -g ndjson
RUN npm install -g JSON
RUN npm install -g zlib
RUN npm install -g util
RUN npm install -g lodash
RUN npm install -g stream

# Tell node where to find dependencies 
ENV NODE_PATH /usr/local/bin
