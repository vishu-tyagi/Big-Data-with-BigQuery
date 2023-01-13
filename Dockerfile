ARG IMAGE
FROM ${IMAGE}

COPY . .

RUN pip install -e src/nyc-taxi

RUN chmod +x ./entrypoint.sh

ENTRYPOINT [ "./entrypoint.sh" ]