BOOTSTRAP_SERVERS = "localhost:9092"

producer_config = {
    "bootstrap.servers": BOOTSTRAP_SERVERS
}
consumer_config = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "group.id": "my-group",
    "auto.offset.reset": "earliest",
}
admin_config = {
    "bootstrap.servers": BOOTSTRAP_SERVERS
}