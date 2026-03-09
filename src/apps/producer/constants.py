GTFS = "gtfs"
GTFS_ACE = "gtfs-ace"
GTFS_BDFM = "gtfs-bdfm"
GTFS_G = "gtfs-g"
GTFS_JZ = "gtfs-jz"
GTFS_NQRW = "gtfs-nqrw"
GTFS_L = "gtfs-l"
GTFS_SI = "gtfs-si"

GTFS_LINES = [GTFS, GTFS_ACE, GTFS_BDFM, GTFS_G, GTFS_JZ, GTFS_NQRW, GTFS_L, GTFS_SI]

GTFS_FEED_URLS: dict[str, str] = {
    GTFS: "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs",
    GTFS_ACE: "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-ace",
    GTFS_BDFM: "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-bdfm",
    GTFS_G: "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-g",
    GTFS_JZ: "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-jz",
    GTFS_NQRW: "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-nqrw",
    GTFS_L: "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-l",
    GTFS_SI: "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-si",
}