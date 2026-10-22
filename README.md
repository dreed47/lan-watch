# LAN watch

A page for how much video Frigate is pulling from the cameras, and for ping loss to those cameras and to ESP devices.

The bitrate comes from go2rtc's byte counter on the Beelink. This app SSHes there and reads the counter. It does not see the packets on the wire. Stream URLs stay on the Beelink. Pings are sent from this container.

## Setup

```sh
cp .env.example .env
cp example-devices.json devices.json
# Edit devices.json. Put a private key at ssh/id_ed25519 for BEELINK_USER.
./restart_container.sh
```

`.env` and `ssh/` are not in git. `devices.json` is the real camera and ESP list, and it is not in git either. `example-devices.json` only shows the shape: a `cameras` list and an `esp` list, each item a `name` and an `ip`.

The image copies `devices.json` when it builds. A list change needs `./restart_container.sh` before the page sees it.

The page listens on port 7852. Inside the container the app uses port 5000. The Docker health check calls `/health`.

Checks start every 60 seconds until someone changes that on the page. Each check measures bitrate for 10 seconds, then sends 5 pings. The page can pause checks or set the gap from 30 seconds to 1 hour. That choice is stored in `data/lan-watch.db` and kept across restarts. The page refreshes itself every few seconds. That refresh is separate from the check gap.

## Update

```sh
./update.sh
./update.sh --no-cache
```

`update.sh` pulls, then rebuilds. `restart_container.sh` rebuilds without pulling.
