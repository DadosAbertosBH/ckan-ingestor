resource "kubernetes_deployment" "example" {
  metadata {
    name      = "terraform-example"
    namespace = "duckdb"
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        test = "duckdb"
      }
    }

    template {
      metadata {
        labels = {
          test = "duckdb"
        }
      }

      spec {
        container {
          image = "datacatering/duckdb:v1.3.1"
          name  = "duckdb"

          resources {
            limits = {
              cpu    = "0.5"
              memory = "512Mi"
            }
            requests = {
              cpu    = "0.5"
              memory = "512Mi"
            }
          }
          port {
            container_port = 4213
          }
        }
      }
    }
  }
}