# Dump1090 Collector

Collects Aircraft info from a piaware device

## Resources 
- https://airmetar.main.jp/radio/ADS-B%20Decoding%20Guide.pdf
- https://github.com/firestuff/adsb-tools/blob/master/protocols/beast.md
- https://mode-s.org/decode/content/ads-b/1-basics.html

# Running with Make
```
make run ARGS="-addr=[addr-of-piaware] -lookupAddr=[some-url] -dbLoc=~/nfs-mnts/dump1090/"
