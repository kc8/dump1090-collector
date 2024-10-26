# Dump1090 Collector

Collects Aircraft data from a piaware device

# Setup 
This service requires other services to run


## Look Up Tail Number Service 
This is a service that returns the Tail Number of the aircraft based on the ICAO from piaware
[example service here](https://github.com/kc8/get-aricraft-data). It needs hit the endpoint [here](https://github.com/kc8/dump1090-collector/blob/11b466570ddde7a75bcbaf8f05a822c564b998a1/main.go#L91)
which should respond with json for the tail number. Example response is below:

    ```json
    {"number": "185DN", "prefix": "N"}
    ```
    ```json
    {"numner": "string", "prefix": "string"}
    ```

## SQL Lite

A SQL lite Database stores history of aircraft. Pass in a location for a SQL lite database to store aircraft in with `-dbLoc=[some-location]`. The database will be created on first run.


## Piware 
Requires a Piaware device 

## Resources 
- https://airmetar.main.jp/radio/ADS-B%20Decoding%20Guide.pdf
- https://github.com/firestuff/adsb-tools/blob/master/protocols/beast.md
- https://mode-s.org/decode/content/ads-b/1-basics.html

# Running with Make
```
make run ARGS="-addr=[addr-of-piaware] -lookupAddr=[some-url] -dbLoc=~/nfs-mnts/dump1090/"
```
