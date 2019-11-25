# build
FROM maven
RUN mkdir /app
WORKDIR /app
COPY ./pom.xml ./
RUN mvn dependency:go-offline

COPY . ./
RUN mvn source:jar install assembly:assembly war:war 

FROM openjdk
COPY ["run_agg.sh", "/"]
COPY --from=0 /app/target/AmazonKinesisAggregators.jar-complete.jar /
CMD /run_agg.sh
