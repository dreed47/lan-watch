# LAN watch

A page for the effect of Frigate's camera streams on the Wi-Fi.

Frigate pulls video from each camera it is using. On a busy wireless network that pull can crowd out other devices. This app shows two things side by side:

- How much video Frigate is pulling, in megabits per second, from go2rtc's own byte counters.
- Ping loss and reply time for the cameras, and for any other devices you list.

The camera names and addresses come from Frigate over that same SSH connection. They are not stored in git. The address is taken from the stream on the Frigate machine, and only the address leaves that machine.

The other devices do not have to be ESP boards. A sensor, a display, a garage controller, or any other host you care about can go in the same list.

The bitrate is read over SSH from the machine that runs Frigate. This app does not see those packets on the wire, and camera stream URLs stay on the Frigate machine. Pings are sent from this container, so they still cross the wireless hop on the way to a Wi-Fi device.

## Setup

```sh
cp .env.example .env
cp example-devices.json devices.json
# Edit .env and devices.json. Put a private key at ssh/id_ed25519 for FRIGATE_USER.
./restart_container.sh
```

`FRIGATE_HOST` is the machine running Frigate. `FRIGATE_USER` is the SSH user on that machine. `.env`, `ssh/`, and `devices.json` are not in git.

`devices.json` is the starting other-device list, under `other`. Each item is a `name` and an `ip`. An older file may say `esp` instead of `other`. That still works. A `cameras` list in that file is ignored, because the camera list comes from Frigate. `example-devices.json` shows the shape.

The first run copies that file to `data/devices.json`. Add, edit, and remove on the page write that copy. Those changes stay across a restart and do not need a rebuild.

The page listens on port 7852. Inside the container the app uses port 5000. The Docker health check calls `/health`.

Checks start every 60 seconds until someone changes that on the page. Each check measures bitrate for 10 seconds, then sends 5 pings. The page can pause checks or set the gap from 30 seconds to 1 hour. That choice is stored in `data/lan-watch.db` and kept across restarts. The page refreshes itself every few seconds. That refresh is separate from the check gap.

## Update

```sh
./update.sh
./update.sh --no-cache
```

`update.sh` pulls, then rebuilds. `restart_container.sh` rebuilds without pulling.
