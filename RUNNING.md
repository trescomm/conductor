# Running conductor locally
## Pre-requisites
* JDK (tested on JDK 13)
* Docker - Get the latest
## Build
* From the conductor root folder run:
> gradlew build
>
>(if the tests are failing or taking too long, to SKIP tests)
>
>gradlew build -x test

## Run server
* After the build completes, run the following from `conductor/server` folder:
> java -jar conductor-server-3.1.0-SNAPSHOT-boot.jar
* Point your browser to http://localhost:8080

## Run UI
* From the `conductor/ui` folder run the following:
> npm install
>
> gulp watch 
