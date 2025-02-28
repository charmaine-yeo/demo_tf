resource "google_bigquery_table" "tables" {
  for_each = {for idx, table in local.tables_flattened : "${table["datasetId"]}_${table["tableId"]}" => table}

  project    = var.project_id
  dataset_id = each.value["datasetId"]
  table_id   = each.value["tableId"]
  clustering = each.value["clustering"]

  dynamic "time_partitioning" {
    for_each = each.value["partitionType"] != null ? [1] : []

    content {
      type                     = each.value["partitionType"]
      field                    = each.value["partitionField"]
      expiration_ms            = each.value["expirationMs"]
      require_partition_filter = each.value["requirePartitionFilter"]
    }
  }

  schema = file("${path.module}/${each.value["tableSchemaPath"]}")
  deletion_protection = false
  labels = try(each.value["tableLabels"], {})
}