run:
    cargo run -- --log --project ../_hidden/etc/GranAlacant.knxproj --mqtt-host=localhost --remote=udp://192.168.8.2  


pub mfile:
    cat {{mfile}} | mqttcli pub -t "knx/group/10/0/1" -s -r -h localhost 