resource "random_password" "redis_password" {
  special = false
  length  = 16
}

resource "helm_release" "redis" {
  name             = "redis"
  chart            = "oci://registry-1.docker.io/bitnamicharts/redis"
  namespace        = "dagster"
  create_namespace = true

  values = [
    yamlencode(
      {
        architecture = "standalone"
        auth = {
          password = random_password.redis_password.result
        }
      }
    )
  ]
}
