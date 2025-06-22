resource "helm_release" "redis" {
  name             = "redis"
  chart            = "oci://registry-1.docker.io/bitnamicharts/redis"
  namespace        = "dagster"
  create_namespace = true

  values = [
    yamlencode(
      {
      }
    )
  ]
}
