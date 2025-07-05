locals {
  dagster_image = {
    repository = "registry.gitlab.com/pedalin/ckan-ingestor"
    tag        = var.image_tag
    pullPolicy = "IfNotPresent"
  }
}