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
          name  = "migrate-v1"
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
  repository       = "https://dadosabertosbh.github.io/dagster"
  chart            = "dagster"
  namespace        = "dagster"
  create_namespace = true
  version          = "0.0.2-dev"

  values = [
    yamlencode(
      {
        global = {
          security = {
            allowInsecureImages = true
          }
        }
        runLauncher = {
          type = "CeleryK8sRunLauncher"
          config = {
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

          usePassword = true
          password    = "XgSsibCa7G"
        }
        rabbitmq = {
          enabled = false
        }
        postgres = {
          primary = {
            extendedConfiguration = <<-EOT
            max_connections = 500
            EOT
          }
        }
        dagsterDaemon = {
          replicaCount = 10
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

              dagster_yaml = yamlencode({
                run_launcher = {
                  module = "dagster_celery_k8s.launcher"
                  class  = "CeleryK8sRunLauncher"
                  config = {
                    broker = {
                      redis_url : "redis://dagster-redis-master:6379/0"
                    }
                    backend = {
                      redis_url : "redis://dagster-redis-master:6379/0"
                    }
                  }
                }
              })

              exector = {
                name = "celery"
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
