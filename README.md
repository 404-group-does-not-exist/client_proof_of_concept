# Wifiology Client Proof of Concept
## Background


## High Level

The wifiology proof of concept uses an 802.11 wireless adapter to listen for wireless traffic, which it aggregates into
statistical data about the amount of traffic present in the current location. The adapter is used to listen for 
beacons for access points (APs) active near by to the system running the Wifiology client. After a suitable number of
802.11 beacon frames have been captured, the client software scans channel by channel for seen APs for a set amount 
of time, capturing data packets. The data packets are analyzed to provide an approximate picture of the number of
devices communicating to each access point and the total volume of traffic. This data in turn is compared against 
historical results to build a relative picture of current congestion.

## Implementation

This proof of concept uses the Python programming language in conjunction with libpcap to do packet capturing.

## Further Reading
### General Informational
* [IEEE 802.11 (Wikipedia)](https://en.wikipedia.org/wiki/IEEE_802.11) 
* [Monitor Mode (Wikipedia)](https://en.wikipedia.org/wiki/Monitor_mode)

### Related Open Source Projects

* [tcpdump/libpcap](https://www.tcpdump.org/)
* [Wireshark](https://www.wireshark.org/)
* [Wifispy](https://github.com/Geovation/wifispy)

