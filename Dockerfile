# build
FROM maven
COPY . /app
WORKDIR /app
RUN mvn clean source:jar install assembly:assembly war:war 

FROM openjdk
COPY ["run_agg.sh", "/"]
COPY --from=0 /app/target/AmazonKinesisAggregators.jar-complete.jar /
CMD /run_agg.sh
