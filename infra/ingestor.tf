resource "kubernetes_cron_job_v1" "ckan_ingestor" {
  metadata {
    name      = "ckan-injestor"
    namespace = "default"
  }
  spec {
    concurrency_policy        = "Replace"
    failed_jobs_history_limit = 5
    #                                  ┌───────────── minute (0 - 59)
    #                                  │ ┌───────────── hour (0 - 23)
    #                                  │ │ ┌───────────── day of the month (1 - 31)
    #                                  │ │ │ ┌───────────── month (1 - 12)
    #                                  │ │ │ │ ┌───────────── day of the week (0 - 6) (Sunday to Saturday)
    #                                  │ │ │ │ │                                   OR sun, mon, tue, wed, thu, fri, sat
    #                                  │ │ │ │ │ 
    #                                  │ │ │ │ │
    #                                  * * * * *    
    schedule                      = "*/5 * * * *"
    starting_deadline_seconds     = 10
    successful_jobs_history_limit = 10
    job_template {
      metadata {}
      spec {
        backoff_limit              = 2
        ttl_seconds_after_finished = 10
        template {
          metadata {}
          spec {
            container {
              name  = "hello"
              image = "registry.gitlab.com/pedalin/ckan-ingestor:6da9a799"
              env {
                name  = "AWS_ACCESS_KEY_ID"
                value = var.s3_access_key
              }
              env {
                name  = "AWS_SECRET_ACCESS_KEY"
                value = var.s3_secret_key
              }
              env {
                name  = "AWS_ENDPOINT"
                value = var.s3_endpoint
              }
              env {
                name  = "AWS_REGION"
                value = var.s3_region
              }
            }
          }
        }
      }
    }
  }
}