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
    "000001_database.up.sql"   = "CREATE DATABASE dagster;"
    "000001_database.down.sql" = ""
  }
}

resource "random_password" "pg_password" {
  special = false
  length  = 16
}

resource "helm_release" "postgresql" {
  name             = "postgresql-dagster"
  chart            = "oci://registry-1.docker.io/bitnamicharts/postgresql"
  create_namespace = true
  namespace        = "dagster"

  set = [
    {
      name  = "auth.postgresPassword"
      value = random_password.pg_password.result
    },
    {
      name  = "primary.extendedConfiguration"
      value = <<-EOT
            max_connections = 500
    EOT
    }
  ]
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
          name  = "migrate-v1"
          image = "migrate/migrate"
          args = [
            "-database",
            "postgres://postgres:${random_password.pg_password.result}@postgresql-dagster/postgres?sslmode=disable",
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

  depends_on = [helm_release.postgresql]
}

resource "helm_release" "dagster" {
  name             = "dagster"
  repository       = "https://dadosabertosbh.github.io/dagster"
  chart            = "dagster"
  namespace        = "dagster"
  create_namespace = true
  version          = "0.0.6-dev"

  values = [
    yamlencode(
      {
        global = {
          security = {
            allowInsecureImages = true
          }
        }
        runLauncher = {
          type = "CustomRunLauncher"
          config = {
            customRunLauncher = {
              module = "dagster._core.launcher"
              class  = "DefaultRunLauncher"
              config = {
              }
            }
            celeryK8sRunLauncher = {
              imagePullPolicy = "IfNotPresent"
              image = {
                repository = "registry.gitlab.com/pedalin/dagster-celery-k8s"
                tag        = "4ecbc1ce"
                pullPolicy = "IfNotPresent"
              }
              resources = {
                requests = {
                  cpu    = "500m"
                  memory = "512Mi"
                }
                limits = {
                  cpu    = "500m"
                  memory = "512Mi"
                }
              }
            }
            k8sRunLauncher = {
              imagePullPolicy = "IfNotPresent"
              resources = {
                requests = {
                  cpu    = "500m"
                  memory = "512Mi"
                }
                limits = {
                  cpu    = "500m"
                  memory = "512Mi"
                }
              }
              runK8sConfig = {
                jobSpecConfig = {
                  ttlSecondsAfterFinished = 600
                }
              }
            }
          }
        }
        redis = {
          enabled  = true
          internal = false

          host            = "redis-master.dagster.svc.cluster.local"
          port            = 6379
          brokerDbNumber  = 0
          backendDbNumber = 0

          usePassword = false
          # password    = random_password.redis_password.result
        }
        rabbitmq = {
          enabled = false
        }
        postgresql = {
          enabled            = false
          postgresqlHost     = "postgresql-dagster"
          postgresqlUsername = "postgres"
          postgresqlPassword = random_password.pg_password.result
          postgresqlDatabase = "dagster"
        }
        dagsterDaemon = {
          image = {
            repository = "registry.gitlab.com/pedalin/dagster-celery-k8s"
            tag        = "4ecbc1ce"
            pullPolicy = "IfNotPresent"
          }
          runCoordinator = {
            config = {
              queuedRunCoordinator = {
                maxConcurrentRuns = 50
              }
            }
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
