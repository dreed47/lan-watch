# LAN watch

A small page for the camera pull into Frigate and for ping loss to the ESP devices.

The bitrate comes from go2rtc's byte counter on the Beelink. This app SSHes to that machine and reads the counter. A container on another computer cannot see those packets on the wire. Pings are sent from this container. They still cross the Eero on the last hop, so loss and delay still show the Wi-Fi.

`update.sh` and `restart_container.sh` are the same operational scripts as [printer-flush-app](https://github.com/dreed47/printer-flush-app). The Docker health check hits `/health`.

## Run

```sh
cp .env.example .env
# ssh/id_ed25519 is a key authorized for david@192.168.86.16
# It is not in git.
./restart_container.sh
```

The page is on port 7852. `./update.sh` pulls and rebuilds.

Device addresses live in `devices.json`.
