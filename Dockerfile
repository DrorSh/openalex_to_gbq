FROM node

RUN npm install -g fast-xml-parser
RUN npm install -g he
RUN npm install -g array-to-ndjson
RUN npm install -g chalk

#RUN apk add --no-cache bash

# install python
#RUN apt-get update -y
#RUN apt-get install -y python


# install schema generator
#RUN pip install --no-cache-dir --upgrade pip && \
#    pip install --no-cache-dir bigquery-schema-generator

# Tell node where to find dependencies 
ENV NODE_PATH /usr/local/bin
