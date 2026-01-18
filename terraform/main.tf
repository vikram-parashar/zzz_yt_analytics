terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.8.0"
    }
  }
}

provider "google" {
  project = "learning-474319"
  region  = "asia-south2"
  zone    = "asia-south2-a"
}

resource "google_bigquery_dataset" "default" {
  dataset_id = "youtube_analytics"
  location   = "EU"
}

resource "google_bigquery_table" "dim_agent" {
  dataset_id = google_bigquery_dataset.default.dataset_id
  table_id   = "dim_agent"

  deletion_protection = true

  schema = jsonencode([
    {
      name = "icon"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "name"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "rank"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "attribute"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "speciality"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "faction"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "release_date"
      type = "DATE"
      mode = "NULLABLE"
    },
    {
      name = "release_version"
      type = "FLOAT64"
      mode = "NULLABLE"
    },
  ])
}

resource "google_bigquery_table" "dim_video" {
  dataset_id = google_bigquery_dataset.default.dataset_id
  table_id   = "dim_video"

  schema = jsonencode([
    {
      name = "video_id"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "title"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "description"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "channel_id"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "channel_title"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "publish_date"
      type = "TIMESTAMP"
      mode = "NULLABLE"
    },
    {
      name = "search_query"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "discovered_at"
      type = "TIMESTAMP"
      mode = "NULLABLE"
    },
  ])
}
