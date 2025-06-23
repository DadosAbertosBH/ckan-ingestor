resource "helm_release" "dagster" {
  name             = "dagster"
  repository       = "https://dagster-io.github.io/helm"
  chart            = "dagster"
  namespace        = "dagster"
  create_namespace = true

  values = [
    yamlencode(
      {
        dagsterDaemon = {
          image = {
            name       = "pedalin/dagster-celery-k8s"
            repository = "registry.gitlab.com"
            tag        = "4ecbc1ce"
            pullPolicy = "IfNotPresent"
          }
        }
        dagsterWebserver = {
          image = {
            name       = "pedalin/ckan-ingestor"
            repository = "registry.gitlab.com"
            tag        = "4ecbc1ce"
            pullPolicy = "IfNotPresent"
          }
        }
        dagster-user-deployments = {
          deployments = [
            {
              name = "ckan-pbh"
              port = 3030
              image = {
                name       = "pedalin/ckan-ingestor"
                repository = "registry.gitlab.com"
                tag        = var.image_tag
                pullPolicy = "IfNotPresent"
              }

              env = [
                {
                  name  = "DUCKLAKE_CATALOG_URI"
                  value = "postgres:dbname=mysql host=${var.ducklake_db_host} database=${var.ducklake_db_database} user=${var.ducklake_db_user} password=${var.ducklake_db_password}"
                },
                {
                  name  = "DUCKLAKE_DATABASE"
                  value = ":memory:"
                },
                {
                  name  = "DUCKLAKE_DATA_PATH__ENDPOINT"
                  value = var.s3_endpoint
                },
                {
                  name  = "DUCKLAKE_DATA_PATH__URL_STYLE"
                  value = "path"
                },
                {
                  name  = "DUCKLAKE_DATA_PATH__ACCESS_KEY_ID"
                  value = var.s3_access_key
                },
                {
                  name  = "DUCKLAKE_DATA_PATH__SECRET_ACCESS_KEY"
                  value = var.s3_secret_key
                }, {
                  name  = "DUCKLAKE_DATA_PATH__BUCKET"
                  value = "public-datasets"
                },
              ]

              dagsterApiGrpcArgs = [
                "--python-file",
                "/app/ckan_dagster/ckan_dagster/definitions.py"
              ]
            }
          ]
        }
      }
    )
  ]
}
