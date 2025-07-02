resource "kubernetes_namespace" "dagster" {
  metadata {
    name = "dagster"
  }
}

resource "kubernetes_config_map_v1" "dagster_config" {
  metadata {
    name      = "migrations"
    namespace = "dagster"
  }
  data = {
    "000001_database.up.sql"   = "CREATE DATABASE ${var.ducklake_db_database};"
    "000001_database.down.sql" = ""
  }
}

resource "kubernetes_job_v1" "initialize_db" {
  metadata {
    name      = "migrations"
    namespace = "dagster"
  }
  spec {
    template {
      metadata {}
      spec {
        container {
          name  = "migrate"
          image = "migrate/migrate"
          args = [
            "-database",
            "postgres://${var.ducklake_db_user}:${var.ducklake_db_password}@${var.ducklake_db_host}/postgres?sslmode=disable",
            "-path",
            "/migrations", "up"
          ]
          volume_mount {
            mount_path = "/migrations"
            name       = "migrations"
          }
        }
        restart_policy = "Never"
        volume {
          name = "migrations"
          config_map {
            name = kubernetes_config_map_v1.dagster_config.metadata[0].name
          }
        }
      }
    }
    backoff_limit = 4
  }
  wait_for_completion = true
  timeouts {
    create = "2m"
    update = "2m"
  }
}

resource "helm_release" "dagster" {
  name             = "dagster"
  repository       = "https://dagster-io.github.io/helm"
  chart            = "dagster"
  namespace        = "dagster"
  create_namespace = true

  values = [
    yamlencode(
      {
        runLauncher = {
          type = "K8sRunLauncher"
          k8sRunLauncher = {
            imagePullPolicy = "IfNotPresent"
            resources = {
              requests = {
                cpu    = "200m"
                memory = "256Mi"
              }
              limits = {
                cpu    = "200m"
                memory = "256Mi"
              }
            }
            runK8sConfig = {
              jobSpecConfig = {
                ttlSecondsAfterFinished = 600
              }
            }
          }
        }
        rabbitmq = {
          enabled = true
        }
        postgres = {
          primary = {
            extendedConfiguration = <<-EOT
            max_connections = 500
            EOT
          }
        }
        dagsterDaemon = {
          image = {
            repository = "registry.gitlab.com/pedalin/dagster-celery-k8s"
            tag        = "4ecbc1ce"
            pullPolicy = "IfNotPresent"
          }
        }
        dagsterWebserver = {
          replicaCount = 2
          image = {
            repository = "registry.gitlab.com/pedalin/dagster-celery-k8s"
            tag        = "4ecbc1ce"
            pullPolicy = "IfNotPresent"
          }
          dbPoolMaxOverflow = 250
        }
        dagster-user-deployments = {
          deployments = [
            {
              name = "ckan-pbh"
              port = 3030
              image = {
                repository = "registry.gitlab.com/pedalin/ckan-ingestor"
                tag        = var.image_tag
                pullPolicy = "IfNotPresent"
              }

              env = [
                {
                  name  = "DUCKLAKE_CATALOG_URI"
                  value = "postgres:host=${var.ducklake_db_host} dbname=${var.ducklake_db_database} user=${var.ducklake_db_user} password=${var.ducklake_db_password}"
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
                },
                {
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

  depends_on = [kubernetes_job_v1.initialize_db]
}
