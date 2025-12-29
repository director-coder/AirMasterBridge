ARG BUILD_FROM
FROM ${BUILD_FROM}

RUN apk add --no-cache python3 py3-pip bash
RUN pip3 install --no-cache-dir paho-mqtt

WORKDIR /app
COPY am7p_bridge.py /app/am7p_bridge.py
COPY run.sh /app/run.sh
RUN chmod +x /app/run.sh

CMD [ "/app/run.sh" ]
