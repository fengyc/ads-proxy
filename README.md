# ADS proxy

[![build-main](https://github.com/fengyc/ads-proxy/actions/workflows/main.yml/badge.svg?branch=main)](https://github.com/fengyc/ads-proxy/actions/workflows/main.yml)

This is a [Beckhoff ADS][ads] proxy that could resolve multiple connections with same ip address issues.

* ads issue: https://github.com/Beckhoff/ADS/issues/49
* pyads issue: https://github.com/stlehmann/pyads/issues/331

[ads]: https://download.beckhoff.com/download/Document/automation/twincat3/TwinCAT_3_ADS_INTRO_EN.pdf

## Usage

Download binary from github release page or `cargo install ads-proxy`, then run `ads-proxy`.

```shell
Usage: ads-proxy [OPTIONS] <PLC_ADDR>

Arguments:
  <PLC_ADDR>  PLC address, e.g. 172.18.0.10:48898

Options:
  -d, --debug                      Debug mode
  -b, --buffer-size <BUFFER_SIZE>  Maximum size of AMS packet [default: 65536]
  -r, --route <ROUTE>              Optional ams net id route, e.g. 10.10.10.10.1.1
  -u, --username <USERNAME>        Optional PLC username (to add route)
  -p, --password <PASSWORD>        Optional PLC password (to add route)
      --host <HOST>                Optional Proxy hostname (hostname or ip address, detected from PLC connection)
  -l, --listen-addr <LISTEN_ADDR>  Proxy listen address [default: 127.0.0.1:48898]
  -h, --help                       Print help
  -V, --version                    Print version
```

In pyads, connect to `ads-proxy`. For example, client 1 and 2 on a host:

* plc `ams_net_id=192.168.0.10.1.1`
* client 1 `ams_net_id=10.10.10.10.1.1`
* client 2 `ams_net_id=10.10.10.10.1.2`

(Might need to add route entry in shell command with `-r` or in PLC configuration)

```shell
ads-proxy -d 192.168.0.10:48898
```

```python
import pyads

pyads.open_port()
pyads.set_local_address("10.10.10.10.1.x")  # x=1 or 2
pyads.close_port()

plc = pyads.Connection(ams_net_id="192.168.0.10.1.1", ams_net_port=851, ip_address="127.0.0.1")
with plc:
    ...
```

## License

MIT
