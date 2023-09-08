package config

// Etcd server hostname
const ETCD_ADDRESS = "etcd.address"

// exposed port for serverledge APIs
const API_PORT = "api.port"
const API_IP = "api.ip"

// REMOTE SERVER URL
const CLOUD_URL = "cloud.server.url"

// Forces runtime container images to be pulled the first time they are used,
// even if they are locally available (true/false).
const FACTORY_REFRESH_IMAGES = "factory.images.refresh"

// Amount of memory available for the container pool (in MB)
const POOL_MEMORY_MB = "container.pool.memory"

// CPUs available for the container pool (1.0 = 1 core)
const POOL_CPUS = "container.pool.cpus"

// periodically janitor wakes up and deletes expired containers
const POOL_CLEANUP_PERIOD = "janitor.interval"

// container expiration time
const CONTAINER_EXPIRATION_TIME = "container.expiration"

// cache capacity
const CACHE_SIZE = "cache.size"

// cache janitor interval (Seconds) : deletes expired items
const CACHE_CLEANUP = "cache.cleanup"

// default expiration time assigned to a cache item (Seconds)
const CACHE_ITEM_EXPIRATION = "cache.expiration"

// default policy is to persist cache (boolean). Use false in localonly deployments
const CACHE_PERSISTENCE = "cache.persistence"

// true if the current server is a remote cloud server
const IS_IN_CLOUD = "cloud"

// the area wich the server belongs to
const REGISTRY_AREA = "registry.area"

// short period: retrieve information about nearby edge-servers
const REG_NEARBY_INTERVAL = "registry.nearby.interval"

// long period for general monitoring inside the area
const REG_MONITORING_INTERVAL = "registry.monitoring.interval"

// registration TTL in seconds
const REGISTRATION_TTL = "registry.ttl"

// port for udp status listener
const LISTEN_UDP_PORT = "registry.udp.port"

// enable metrics system
const METRICS_ENABLED = "metrics.enabled"

const METRICS_PROMETHEUS_HOST = "metrics.prometheus.host"
const METRICS_PROMETHEUS_PORT = "metrics.prometheus.port"

// Scheduling policy to use
// Possible values: "qosaware", "default", "cloudonly"
const SCHEDULING_POLICY = "scheduler.policy"

// Capacity of the queue (possibly) used by the scheduler
const SCHEDULER_QUEUE_CAPACITY = "scheduler.queue.capacity"

// Enables tracing
const TRACING_ENABLED = "tracing.enabled"

// Custom output file for traces
const TRACING_OUTFILE = "tracing.outfile"
