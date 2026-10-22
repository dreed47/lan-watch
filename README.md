# Frigate Load

A monitor for people who run Frigate with Wi-Fi cameras and want to see what those cameras are doing to the network.

Wi-Fi cameras send a steady stream of video to Frigate. On a shared network that stream can crowd out other devices. Frigate Load shows both sides of that:

- How much video Frigate is receiving, in megabits per second.
- Ping loss and reply time for the cameras, and for any other devices you add.

It does not listen to the Wi-Fi radio, and it does not cap the cameras. The bitrate comes from go2rtc's own counters on the machine running Frigate, read over SSH. Camera stream URLs stay on that machine. Pings are sent from this app, so a Wi-Fi device still has to answer across the wireless hop.

## Setup

```sh
cp .env.example .env
cp example-devices.json devices.json
# Edit .env. Put a private key at ssh/id_ed25519 for FRIGATE_USER.
./restart_container.sh
```

`FRIGATE_HOST` is the machine running Frigate. `FRIGATE_USER` is an SSH user on that machine. `.env`, `ssh/`, and `devices.json` are not in git.

Camera names and addresses come from Frigate on each check. `devices.json` is only the starting list of other devices, under `other`. Each item is a `name` and an `ip`. An older file may say `esp` instead of `other`. That still works. A `cameras` list in that file is ignored. `example-devices.json` shows the shape.

The first run copies that file to `data/devices.json`. Add, edit, and remove on the page write that copy. Those changes stay across a restart and do not need a rebuild.

The page listens on port 7852. Inside the container the app uses port 5000. The Docker health check calls `/health`.

Measurements start every 60 seconds until someone changes that on the page. Each one reads camera traffic for 10 seconds, then sends 5 pings. The page can pause measuring or set the gap from 30 seconds to 1 hour. That choice is stored in `data/lan-watch.db` and kept across restarts. The page refreshes itself every few seconds. That refresh is separate from the measurement gap.

## Update

```sh
./update.sh
./update.sh --no-cache
```

`update.sh` pulls, then rebuilds. `restart_container.sh` rebuilds without pulling.
